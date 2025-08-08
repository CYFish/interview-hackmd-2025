import argparse
import json
import logging
import os
import glob
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

import boto3
import pandas as pd
import pendulum
import pyarrow.parquet as pq
import pyarrow as pa


# Configure logging format with timestamp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ArxivProcessor:
    """
    A processor for ArXiv metadata JSON files.

    Processes JSON files containing ArXiv paper metadata collected by collector.py,
    cleans the data, and converts it to Parquet format for efficient storage and analysis.
    """

    def __init__(
        self,
        from_date: str,
        to_date: str,
        input_dir: str,
        output_path: str,
        formats: List[str] = ["arXiv", "arXivRaw"],
        local_mode: bool = False,
        bucket: str = "hackmd-project-2025",
        chunk_size: int = 10000
    ):
        """
        Initialize the ArXiv processor.

        Args:
            from_date: Start date in ISO format (YYYY-MM-DD), inclusive
            to_date: End date in ISO format (YYYY-MM-DD), exclusive
            input_dir: Directory containing input files (local path or S3 prefix)
            output_path: Path for the output Parquet file (S3 prefix or local directory)
            formats: List of metadata formats to process (arXiv, arXivRaw)
            local_mode: Whether to use local file system instead of S3
            bucket: S3 bucket name (only used if local_mode is False)
            chunk_size: Number of records to process in each batch
        """
        self.local_mode = local_mode
        self.input_dir = input_dir
        self.output_path = output_path
        self.formats = formats
        self.chunk_size = chunk_size
        self.bucket = bucket

        try:
            # Parse dates
            self.from_date = pendulum.parse(from_date)
            self.to_date = pendulum.parse(to_date)

            logger.info(f"Processing data from {self.from_date.to_date_string()} (inclusive) to {self.to_date.to_date_string()} (exclusive)")

            # Validate date range
            if self.from_date >= self.to_date:
                raise ValueError("from_date must be earlier than to_date")
        except Exception as e:
            logger.error(f"Error parsing from_date or to_date: {e}")
            raise

        if not local_mode:
            self.s3 = boto3.client("s3", region_name="ap-northeast-1")
            logger.info(f"Using S3 storage: bucket={self.bucket}")
        else:
            logger.info(f"Using local storage")
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

        self.buffers = {}
        self.counters = {}
        self.is_first_batch = True

        # Enhanced schema based on project requirements
        self.schema = pa.schema([
            # Core paper metadata
            ("id", pa.string()),  # arXiv ID
            ("title", pa.string()),
            ("abstract", pa.string()),
            ("categories", pa.string()),  # Subject categories
            ("doi", pa.string()),  # Digital Object Identifier

            # Temporal data (for trend analysis)
            ("submitted_date", pa.date32()),  # Initial submission date
            ("last_updated_date", pa.date32()),  # Last update date
            ("version_count", pa.int32()),  # Number of versions/updates

            # Author and institution data (for rankings and networks)
            ("authors", pa.string()),  # Full author list
            ("authors_parsed", pa.list_(pa.list_(pa.string()))),  # Structured author data
            ("institutions", pa.string()),  # Extracted from submitter when available

            # Publication data
            ("journal_ref", pa.string()),  # Journal reference if published
            ("is_published", pa.bool_()),  # Whether paper has been published in a journal

            # Additional fields for analytics
            ("update_frequency", pa.float32()),  # Derived: days between updates
            ("submission_to_publication", pa.float32()),  # Derived: days from submission to publication
        ])

        # Processing statistics
        self.stats = {
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "start_time": None,
            "end_time": None,
            "processed_files": [],
            "data_quality": {
                "missing_titles": 0,
                "missing_abstracts": 0,
                "missing_categories": 0,
                "missing_authors": 0,
                "anomalies": []
            }
        }

    def _get_date_range(self):
        """
        Get a list of dates in the specified range.

        Returns:
            List of date strings in ISO format (YYYY-MM-DD)
        """
        dates = []
        current_date = self.from_date.date()
        end_date = self.to_date.date()

        while current_date < end_date:
            dates.append(current_date.isoformat())
            current_date = current_date.add(days=1)

        return dates

    def _list_input_files(self):
        """
        List all input files for the specified date range and formats.

        Returns:
            Dictionary mapping format to list of input files
        """
        input_files = {}
        dates = self._get_date_range()

        for fmt in self.formats:
            input_files[fmt] = []

            for date_str in dates:
                if self.local_mode:
                    # List local files
                    date_dir = os.path.join(self.input_dir, fmt, date_str)
                    if os.path.exists(date_dir):
                        files = glob.glob(os.path.join(date_dir, "*.json"))
                        input_files[fmt].extend(files)
                else:
                    # List S3 files
                    prefix = f"{self.input_dir}/{fmt}/{date_str}/"
                    try:
                        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
                        if "Contents" in response:
                            for obj in response["Contents"]:
                                if obj["Key"].endswith(".json"):
                                    input_files[fmt].append(obj["Key"])
                    except Exception as e:
                        logger.error(f"Error listing S3 files for {prefix}: {e}")

        return input_files

    def _parse_arxiv_date(self, date_str: str) -> Optional[pendulum.DateTime]:
        """
        Parse various date formats from arXiv records.

        Args:
            date_str: Date string in various formats

        Returns:
            Parsed pendulum DateTime object or None if parsing fails
        """
        if not date_str:
            return None

        try:
            # Try standard ISO format first
            return pendulum.parse(date_str)
        except Exception:
            pass

        try:
            # Try RFC 2822 format (used in arXivRaw versions)
            # Example: "Mon, 28 Sep 2009 12:45:46 GMT"
            dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")
            return pendulum.instance(dt)
        except Exception:
            pass

        try:
            # Try to extract date from string with regex
            # Look for patterns like YYYY-MM-DD or DD MMM YYYY
            date_patterns = [
                r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
                r'(\d{2} \w{3} \d{4})',   # DD MMM YYYY
            ]

            for pattern in date_patterns:
                match = re.search(pattern, date_str)
                if match:
                    extracted_date = match.group(1)
                    return pendulum.parse(extracted_date)
        except Exception:
            pass

        # If all parsing attempts fail
        logger.warning(f"Unable to parse date string: {date_str}")
        return None

    def clean_data(self, record: Dict[str, Any], format_type: str) -> Dict[str, Any]:
        """
        Clean and transform a single JSON record.

        Args:
            record: Raw JSON record from ArXiv metadata
            format_type: Format type (arXiv or arXivRaw)

        Returns:
            Cleaned record with standardized fields
        """
        # Handle ID
        record_id = record.get("id", "").strip() if isinstance(record.get("id"), str) else str(record.get("id", ""))

        # Basic quality checks
        if not record_id:
            logger.warning("Record missing ID, generating placeholder")
            record_id = f"unknown_{pendulum.now().timestamp()}"

        # Initialize result with default values
        result = {
            "id": record_id,
            "title": "",
            "abstract": "",
            "categories": "",
            "doi": "",
            "submitted_date": None,
            "last_updated_date": None,
            "version_count": 0,
            "authors": "",
            "authors_parsed": [],
            "institutions": "",
            "journal_ref": "",
            "is_published": False,
            "update_frequency": None,
            "submission_to_publication": None,
        }

        # Process format-specific fields
        if format_type == "arXiv":
            # Handle title
            title = record.get("title", "").strip() if isinstance(record.get("title"), str) else ""
            if not title:
                self.stats["data_quality"]["missing_titles"] += 1
            result["title"] = title

            # Handle abstract
            abstract = record.get("abstract", "").strip() if isinstance(record.get("abstract"), str) else ""
            if not abstract:
                self.stats["data_quality"]["missing_abstracts"] += 1
            result["abstract"] = abstract

            # Handle categories
            categories = record.get("categories", "").strip() if isinstance(record.get("categories"), str) else ""
            if not categories:
                self.stats["data_quality"]["missing_categories"] += 1
            result["categories"] = categories

            # Handle DOI
            result["doi"] = record.get("doi", "").strip() if isinstance(record.get("doi"), str) else ""

            # Handle journal reference and publication status
            journal_ref = record.get("journal_ref", "").strip() if isinstance(record.get("journal_ref"), str) else ""
            result["journal_ref"] = journal_ref
            result["is_published"] = bool(journal_ref)

            # Process author information
            if "authors_parsed" in record and isinstance(record["authors_parsed"], list):
                result["authors_parsed"] = record["authors_parsed"]

                # Create authors string from parsed authors
                author_names = []
                for author in record["authors_parsed"]:
                    if isinstance(author, list) and len(author) >= 2:
                        name = f"{author[0]}, {author[1]}"
                        author_names.append(name)

                if author_names:
                    result["authors"] = "; ".join(author_names)

            # If we have keyname and forenames, use them
            elif "keyname" in record and "forenames" in record:
                authors_parsed = []
                author_names = []

                for i in range(min(len(record["keyname"]), len(record["forenames"]))):
                    lastname = record["keyname"][i]
                    firstname = record["forenames"][i]
                    authors_parsed.append([lastname, firstname, ""])
                    author_names.append(f"{lastname}, {firstname}")

                result["authors_parsed"] = authors_parsed
                result["authors"] = "; ".join(author_names)

            # Handle dates
            update_date = record.get("update_date")
            if update_date:
                date = self._parse_arxiv_date(update_date)
                if date:
                    result["submitted_date"] = date.date()
                    result["last_updated_date"] = date.date()

        elif format_type == "arXivRaw":
            # Process authors from arXivRaw format
            if "authors" in record and isinstance(record["authors"], str):
                result["authors"] = record["authors"]
            else:
                self.stats["data_quality"]["missing_authors"] += 1

            # Try to extract institution from submitter
            if "submitter" in record and isinstance(record["submitter"], str):
                result["institutions"] = record["submitter"]

            # Process versions and dates
            if "versions" in record and isinstance(record["versions"], list) and record["versions"]:
                result["version_count"] = len(record["versions"])

                # First version is submission date
                if len(record["versions"]) > 0 and "created" in record["versions"][0]:
                    first_date = self._parse_arxiv_date(record["versions"][0]["created"])
                    if first_date:
                        result["submitted_date"] = first_date.date()

                # Last version is last update date
                if len(record["versions"]) > 0 and "created" in record["versions"][-1]:
                    last_date = self._parse_arxiv_date(record["versions"][-1]["created"])
                    if last_date:
                        result["last_updated_date"] = last_date.date()

        # Common processing for both formats

        # Use datestamp from OAI-PMH header if dates are still missing
        if not result["submitted_date"] and "datestamp" in record:
            datestamp = self._parse_arxiv_date(record["datestamp"])
            if datestamp:
                result["submitted_date"] = datestamp.date()
                if not result["last_updated_date"]:
                    result["last_updated_date"] = datestamp.date()

        # Calculate derived metrics
        if result["submitted_date"] and result["last_updated_date"] and result["version_count"] > 1:
            # Calculate update frequency (days between first submission and last update)
            days_diff = (result["last_updated_date"] - result["submitted_date"]).days
            if days_diff > 0 and result["version_count"] > 1:
                result["update_frequency"] = days_diff / (result["version_count"] - 1)

        # Calculate time from submission to publication (if published)
        if result["is_published"] and result["submitted_date"] and result["journal_ref"]:
            # Try to extract publication date from journal reference
            try:
                # Look for year in journal reference (e.g., "Journal Name (2022)")
                year_match = re.search(r'\((\d{4})\)', result["journal_ref"])
                if year_match:
                    pub_year = int(year_match.group(1))
                    # Approximate publication date as middle of the year
                    pub_date = pendulum.date(pub_year, 7, 1)
                    days_to_pub = (pub_date - result["submitted_date"]).days
                    if days_to_pub >= 0:  # Only if publication is after submission
                        result["submission_to_publication"] = days_to_pub
            except Exception as e:
                logger.debug(f"Could not calculate submission to publication time: {e}")

        # Check for critical missing data
        if not result["submitted_date"]:
            # Use current date as last resort
            today = pendulum.today().date()
            result["submitted_date"] = today
            if not result["last_updated_date"]:
                result["last_updated_date"] = today

        if not result["last_updated_date"]:
            result["last_updated_date"] = result["submitted_date"]

        if not result["version_count"]:
            if result["submitted_date"] != result["last_updated_date"]:
                result["version_count"] = 2  # At least 2 versions if dates differ
            else:
                result["version_count"] = 1  # Default to 1

        return result

    def process(self) -> Dict[str, Any]:
        """
        Main processing pipeline: read, clean, and save data in batches.

        Reads the JSON files for the specified date range, processes each record,
        and saves batches as Parquet files.

        Returns:
            Statistics about the processing
        """
        self.stats["start_time"] = pendulum.now().to_datetime_string()
        total_processed = 0

        try:
            # Get list of input files to process
            input_files = self._list_input_files()
            total_files = sum(len(files) for files in input_files.values())
            logger.info(f"Found {total_files} files to process across {len(self.formats)} formats")

            # Process each format
            for fmt, files in input_files.items():
                logger.info(f"Processing {len(files)} {fmt} files")

                # Process each file
                for file_path in files:
                    try:
                        file_processed_count = self._process_file(file_path, fmt)
                        total_processed += file_processed_count
                        self.stats["processed_files"].append(file_path)
                    except Exception as e:
                        logger.error(f"Error processing file {file_path}: {e}")

            # Process remaining batches
            for partition_key, cleaned_records in list(self.buffers.items()):
                if cleaned_records:
                    self._write_parquet(partition_key, cleaned_records)
                    total_processed += len(cleaned_records)

            self.stats["end_time"] = pendulum.now().to_datetime_string()
            logger.info(f"Processing complete. Total records processed: {total_processed}")

            # Generate quality report
            quality_report = self._generate_quality_report()

            return {
                "status": "success",
                "total_records": self.stats["total_records"],
                "successful_records": self.stats["successful_records"],
                "failed_records": self.stats["failed_records"],
                "processed_files": len(self.stats["processed_files"]),
                "data_quality": quality_report,
                "start_time": self.stats["start_time"],
                "end_time": self.stats["end_time"]
            }

        except Exception as e:
            self.stats["end_time"] = pendulum.now().to_datetime_string()
            logger.error(f"Error in processing pipeline: {e}")

            return {
                "status": "error",
                "error": str(e),
                "total_records": self.stats["total_records"],
                "successful_records": self.stats["successful_records"],
                "failed_records": self.stats["failed_records"],
                "processed_files": len(self.stats["processed_files"]),
                "start_time": self.stats["start_time"],
                "end_time": self.stats["end_time"]
            }

    def _process_file(self, file_path: str, format_type: str) -> int:
        """
        Process a single JSON file.

        Args:
            file_path: Path to the JSON file
            format_type: Format type (arXiv or arXivRaw)

        Returns:
            Number of records processed
        """
        processed_count = 0

        try:
            # Read file
            if self.local_mode:
                logger.info(f"Reading from local file: {file_path}")
                with open(file_path, 'r', encoding='utf-8') as file:
                    raw_lines = file.readlines()
            else:
                logger.info(f"Reading from S3: s3://{self.bucket}/{file_path}")
                file_object = self.s3.get_object(Bucket=self.bucket, Key=file_path)
            raw_lines = file_object["Body"].iter_lines()

                # Process each line
            for i, line in enumerate(raw_lines):
                if not line or (isinstance(line, bytes) and not line.strip()) or (isinstance(line, str) and not line.strip()):
                    continue

                try:
                    # Parse JSON
                    if isinstance(line, bytes):
                        record = json.loads(line.decode('utf-8'))
                    else:
                        record = json.loads(line)

                        # Clean data
                        cleaned = self.clean_data(record, format_type)

                        # Skip records with critical missing data
                        if (format_type == "arXiv" and
                            not cleaned["title"] and
                            not cleaned["abstract"] and
                            not cleaned["categories"]):
                            logger.warning(f"Skipping record {cleaned['id']} with insufficient data")
                            self.stats["failed_records"] += 1
                            continue

                        # Determine partition key based on submission date
                        submitted_date = cleaned["submitted_date"]
                        if submitted_date:
                            partition_key = f"year={submitted_date.year}/month={submitted_date.month:02}/day={submitted_date.day:02}"
                        else:
                            # Fallback to current date if no submission date
                            today = pendulum.now().date()
                            partition_key = f"year={today.year}/month={today.month:02}/day={today.day:02}"

                        # Add to buffer
                        if partition_key not in self.buffers:
                            self.buffers[partition_key] = []
                            self.counters[partition_key] = 0

                            self.buffers[partition_key].append(cleaned)
                            self.stats["total_records"] += 1
                            self.stats["successful_records"] += 1
                            processed_count += 1

                        # Write batch when buffer is full
                        if len(self.buffers[partition_key]) >= self.chunk_size:
                            self._write_parquet(partition_key, self.buffers[partition_key])
                            logger.info(f"Processed {processed_count} records from {file_path}")
                            self.buffers[partition_key] = []

                except Exception as e:
                    logger.error(f"Error processing record at line {i + 1} in {file_path}: {e}")
                    self.stats["failed_records"] += 1
                    continue

            logger.info(f"Completed processing {file_path}: {processed_count} records")
            return processed_count

        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return 0

    def _write_parquet(self, partition_key: str, batch: List[Dict[str, Any]]) -> None:
        """
        Write a batch of records to Parquet format.

        Args:
            partition_key: Partition key for the output file
            batch: List of cleaned records
        """
        try:
            # Convert to DataFrame
            df = pd.DataFrame(batch)

            # Handle nested lists for authors_parsed
            if 'authors_parsed' in df.columns:
                # Ensure authors_parsed is properly formatted for PyArrow
                df['authors_parsed'] = df['authors_parsed'].apply(
                    lambda x: x if isinstance(x, list) else []
                )

            # Update counter
            if partition_key not in self.counters:
                self.counters[partition_key] = 1
            else:
                self.counters[partition_key] += 1

                # Convert to PyArrow Table - use schema inference instead of fixed schema
                # to handle potential missing columns
                table = pa.Table.from_pandas(df)

                if self.local_mode:
                    # Write to local file system
                    partition_dir = os.path.join(self.output_path, partition_key)
                    os.makedirs(partition_dir, exist_ok=True)
                    file_name = f"part-{self.counters[partition_key]:05d}.parquet"
                    file_path = os.path.join(partition_dir, file_name)
                    pq.write_table(table, file_path)
                    logger.info(f"✓ Wrote {len(batch)} records to {file_path}")
                else:
                    # Write to S3
                    partition_path = f"{self.output_path}/{partition_key}"
                    file_name = f"part-{self.counters[partition_key]:05d}.parquet"
                    object_key = f"{partition_path}/{file_name}"

                    # Write to buffer and then to S3
                    with pa.BufferOutputStream() as stream:
                        pq.write_table(table, stream)
                        data = stream.getvalue()

                        self.s3.put_object(
                            Bucket=self.bucket,
                            Key=object_key,
                            Body=data.to_pybytes()
                        )

                    logger.info(f"✓ Wrote {len(batch)} records to s3://{self.bucket}/{object_key}")

        except Exception as e:
            logger.error(f"Error writing Parquet file: {e}")
            raise

    def _generate_quality_report(self) -> Dict[str, Any]:
        """
        Generate a data quality report.

        Returns:
            Dictionary with data quality metrics
        """
        total_records = self.stats["total_records"]
        if total_records == 0:
            return {
                "missing_titles_pct": 0,
                "missing_abstracts_pct": 0,
                "missing_categories_pct": 0,
                "missing_authors_pct": 0,
                "anomalies": self.stats["data_quality"]["anomalies"]
            }

        return {
            "missing_titles_pct": round(self.stats["data_quality"]["missing_titles"] / total_records * 100, 2),
            "missing_abstracts_pct": round(self.stats["data_quality"]["missing_abstracts"] / total_records * 100, 2),
            "missing_categories_pct": round(self.stats["data_quality"]["missing_categories"] / total_records * 100, 2),
            "missing_authors_pct": round(self.stats["data_quality"]["missing_authors"] / total_records * 100, 2),
            "anomalies": self.stats["data_quality"]["anomalies"]
        }


def main():
    """
    Main function for local execution.
    """
    # Set up command line arguments
    parser = argparse.ArgumentParser(description="Process ArXiv metadata")
    parser.add_argument("--from-date", type=str, required=True,
                        help="Start date (YYYY-MM-DD), inclusive")
    parser.add_argument("--to-date", type=str, required=True,
                        help="End date (YYYY-MM-DD), exclusive")
    parser.add_argument("--input-dir", type=str, required=True,
                        help="Directory containing input files (local path or S3 prefix)")
    parser.add_argument("--output-path", type=str, required=True,
                        help="Path for output Parquet files (S3 prefix or local directory)")
    parser.add_argument("--formats", type=str, default="arXiv,arXivRaw",
                        help="Comma-separated list of formats to process (default: arXiv,arXivRaw)")
    parser.add_argument("--local-mode", action="store_true",
                        help="Use local file system instead of S3")
    parser.add_argument("--bucket", type=str, default="hackmd-project-2025",
                        help="S3 bucket name (only used if not in local mode)")
    parser.add_argument("--chunk-size", type=int, default=10000,
                        help="Number of records to process in each batch")

    args = parser.parse_args()

    # Parse formats
    formats = args.formats.split(",")

    # Create processor
    processor = ArxivProcessor(
        from_date=args.from_date,
        to_date=args.to_date,
        input_dir=args.input_dir,
        output_path=args.output_path,
        formats=formats,
        local_mode=args.local_mode,
        bucket=args.bucket,
        chunk_size=args.chunk_size
    )

    # Run processing
    result = processor.process()

    # Display results
    print("\nProcessing completed!")
    print(f"Status: {result['status']}")
    print(f"Total records: {result['total_records']}")
    print(f"Successful records: {result['successful_records']}")
    print(f"Failed records: {result['failed_records']}")
    print(f"Files processed: {result['processed_files']}")
    print(f"Start time: {result['start_time']}")
    print(f"End time: {result['end_time']}")

    # Print data quality metrics
    print("\nData Quality Report:")
    print(f"Missing titles: {result['data_quality']['missing_titles_pct']}%")
    print(f"Missing abstracts: {result['data_quality']['missing_abstracts_pct']}%")
    print(f"Missing categories: {result['data_quality']['missing_categories_pct']}%")
    print(f"Missing authors: {result['data_quality']['missing_authors_pct']}%")
    if result['data_quality']['anomalies']:
        print(f"Anomalies detected: {len(result['data_quality']['anomalies'])}")


if __name__ == "__main__":
    main()
