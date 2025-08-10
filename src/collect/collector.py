import argparse
import json
import logging
import os
import shutil
from typing import Dict, List, Any, Optional

from sickle import Sickle
from sickle.models import Record

import boto3
import pendulum


# Configure logging format with timestamp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ArxivCollector:
    """
    A collector for ArXiv metadata.

    Collects data from the arXiv API and saves it as raw JSON to local file or S3.
    """
    def __init__(
        self,
        from_str: str,
        to_str: str,
        local_dir: Optional[str] = None,
        use_s3: bool = False,
        bucket: str = "hackmd-project-2025",
        batch_size: int = 10000
    ):
        """
        Initialize the ArXiv collector.

        Args:
            from_str: Start date in ISO format (YYYY-MM-DD), inclusive
            to_str: End date in ISO format (YYYY-MM-DD), exclusive
            local_dir: Local directory for storing data (if not using S3)
            use_s3: Whether to use S3 for storage
            bucket: S3 bucket name (only used if use_s3 is True)
            batch_size: Number of records to process in each batch
        """
        self.use_s3 = use_s3
        self.local_dir = local_dir or "data"
        self.batch_size = batch_size

        if use_s3:
            self.s3 = boto3.client("s3", region_name="ap-northeast-1")
            self.bucket = bucket
            logger.info(f"Using S3 storage: bucket={self.bucket}")
        else:
            os.makedirs(self.local_dir, exist_ok=True)
            logger.info(f"Using local storage: directory={self.local_dir}")

        try:
            # Parse dates
            self.from_date = pendulum.parse(from_str)
            self.to_date = pendulum.parse(to_str)

            logger.info(f"Collecting data from {self.from_date.to_date_string()} to {self.to_date.to_date_string()}")

            # Validate date range
            if self.from_date >= self.to_date:
                raise ValueError("from_date must be earlier than to_date")
        except Exception as e:
            logger.error(f"Error parsing from_date or to_date: {e}")
            raise

        # Collection statistics
        self.stats = {
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "start_time": None,
            "end_time": None
        }

        # Organize records by datestamp for storage
        self.records_by_date = {}

        # File sequence counters for each date and format
        self.sequence_counters = {}

    def collect(self) -> Dict[str, Any]:
        """
        Main collection process.

        Collects data from arXiv OAI-PMH API and saves it to local file or S3.
        Records with datestamp >= from_date and < to_date will be collected.

        Returns:
            Statistics about the collection process
        """
        self.stats["start_time"] = pendulum.now().to_datetime_string()

        # Always delete existing data before collection to avoid duplicates
        logger.info("Deleting existing data before collection")
        self._delete_existing_data()

        sickle = Sickle("https://oaipmh.arxiv.org/oai")

        params = {
            "from": self.from_date.to_date_string(),
            "until": self.to_date.subtract(days=1).to_date_string(),  # OAI-PMH 'until' is inclusive
        }

        logger.info("Starting arXiv metadata collection")

        metadata_formats = ["arXiv", "arXivRaw"]
        for metadata_format in metadata_formats:
            logger.info(f"Collecting {metadata_format} metadata")

            # Initialize records by date for this format
            self.records_by_date[metadata_format] = {}

            # Initialize sequence counters for this format
            if metadata_format not in self.sequence_counters:
                self.sequence_counters[metadata_format] = {}

            try:
                params["metadataPrefix"] = metadata_format
                records = sickle.ListRecords(**params)

                for record in records:
                    self.stats["total_records"] += 1

                    try:
                        # Process record
                        processed_record = self._process_record(record, metadata_format)

                        # Get datestamp for organizing
                        datestamp = processed_record.get("datestamp")
                        if not datestamp:
                            logger.warning(f"Record missing datestamp: {processed_record.get('id', 'unknown')}")
                            self.stats["failed_records"] += 1
                            continue

                        # Check if datestamp is within our range (>= from_date and < to_date)
                        record_date = pendulum.parse(datestamp).date()
                        if record_date < self.from_date.date() or record_date >= self.to_date.date():
                            logger.debug(f"Skipping record with datestamp {datestamp} (outside requested range)")
                            continue

                        # Add record to appropriate date bucket
                        date_str = record_date.isoformat()
                        if date_str not in self.records_by_date[metadata_format]:
                            self.records_by_date[metadata_format][date_str] = []

                        # Initialize sequence counter for this date if needed
                        if date_str not in self.sequence_counters[metadata_format]:
                            self.sequence_counters[metadata_format][date_str] = 1

                        self.records_by_date[metadata_format][date_str].append(processed_record)
                        self.stats["successful_records"] += 1

                        # If we have enough records for a date, save them
                        if len(self.records_by_date[metadata_format][date_str]) >= self.batch_size:
                            self._save_date_batch(metadata_format, date_str)

                    except Exception as e:
                        logger.error(f"Error processing record: {e}")
                        self.stats["failed_records"] += 1

                # Save remaining records for each date
                for date_str in list(self.records_by_date[metadata_format].keys()):
                    if self.records_by_date[metadata_format][date_str]:
                        self._save_date_batch(metadata_format, date_str)

            except Exception as e:
                logger.error(f"Error collecting {metadata_format} metadata: {e}")

        self.stats["end_time"] = pendulum.now().to_datetime_string()
        logger.info(f"Collection completed. Stats: {self.stats}")

        return self.stats

    def _delete_existing_data(self) -> None:
        """
        Delete existing data for the date range before collection.

        This is always done before collection to ensure data consistency and avoid duplicates.
        For local storage: Removes files in the date directories.
        For S3: Deletes objects with matching date prefixes.
        """
        for file_date in pendulum.interval(self.from_date, self.to_date.subtract(days=1)):
            if self.use_s3:
                # For S3, delete the objects with matching date prefixes
                logger.info(f"Deleting existing data for {file_date.to_date_string()}")
                arxiv_deleted_count = self._delete_existing_data_s3("arXiv", file_date)
                arxiv_raw_deleted_count = self._delete_existing_data_s3("arXivRaw", file_date)
                logger.info(f"Deleted {arxiv_deleted_count + arxiv_raw_deleted_count} objects from S3")
            else:
                # For local storage, delete the directory
                arxiv_date_dir = os.path.join(self.local_dir, "raw", "arXiv", file_date.to_date_string())
                arxiv_raw_date_dir = os.path.join(self.local_dir, "raw", "arXivRaw", file_date.to_date_string())

                if os.path.exists(arxiv_date_dir):
                    shutil.rmtree(arxiv_date_dir)
                if os.path.exists(arxiv_raw_date_dir):
                    shutil.rmtree(arxiv_raw_date_dir)

                logger.info(f"Deleted existing files in {arxiv_date_dir} and {arxiv_raw_date_dir}")

    def _delete_existing_data_s3(self, format: str, file_date: pendulum.DateTime) -> int:
        """
        Delete existing data for a specific date from S3.

        Args:
            format: The format of the data to delete (arXiv or arXivRaw)
            file_date: The date to delete the data for

        Returns:
            The number of objects deleted
        """
        prefix = f"raw/{format}/{file_date.to_date_string()}/"
        response = self.s3.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix,
        )

        if "Contents" in response:
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
            self.s3.delete_objects(
                Bucket=self.bucket,
                Delete={
                    "Objects": objects_to_delete,
                    "Quiet": True,
                },
            )

            return len(objects_to_delete)
        else:
            return 0

    def _process_record(self, record: Record, metadata_format: str) -> Dict[str, Any]:
        """
        Process a single record from arXiv.

        Args:
            record: OAI-PMH record
            metadata_format: Metadata format

        Returns:
            Processed record as dictionary
        """
        metadata = record.metadata
        header = record.header

        if metadata_format == "arXiv":
            return self._process_arxiv_record(metadata, header)
        elif metadata_format == "arXivRaw":
            return self._process_arxiv_raw_record(metadata, header)
        else:
            logger.warning(f"Unknown metadata format: {metadata_format}")
            return {}

    def _process_arxiv_record(self, metadata: Dict[str, List[str]], header: Any) -> Dict[str, Any]:
        """
        Process arXiv format record.
        """
        try:
            # Process core fields
            result = {
                "id": metadata.get("id", [""])[0],
                "title": metadata.get("title", [""])[0],
                "categories": metadata.get("categories", [""])[0],
                "abstract": metadata.get("abstract", [""])[0],
                "update_date": metadata.get("updated", [""])[0],
                "oai_identifier": header.identifier,
                "datestamp": header.datestamp,
                "setSpecs": list(header.setSpecs)
            }

            # Process optional fields
            if "doi" in metadata:
                result["doi"] = metadata["doi"][0]

            if "journal-ref" in metadata:
                result["journal_ref"] = metadata["journal-ref"][0]

            # Process author information
            if "keyname" in metadata and "forenames" in metadata:
                result["authors_parsed"] = [
                    [pair[0], pair[1], ""]
                    for pair in zip(metadata["keyname"], metadata["forenames"])
                ]

            return result
        except Exception as e:
            logger.error(f"Error processing arXiv record: {e}")
            return {}

    def _process_arxiv_raw_record(self, metadata: Dict[str, List[str]], header: Any) -> Dict[str, Any]:
        """
        Process arXivRaw format record.
        """
        try:
            # Process core fields
            result = {
                "id": metadata.get("id", [""])[0],
                "oai_identifier": header.identifier,
                "datestamp": header.datestamp,
                "setSpecs": list(header.setSpecs)
            }

            # Process optional fields
            if "submitter" in metadata:
                result["submitter"] = metadata["submitter"][0]

            if "authors" in metadata:
                result["authors"] = metadata["authors"][0]

            # Process version information
            if "version" in metadata and "date" in metadata:
                result["versions"] = sorted(
                    [{"version": pair[0], "created": pair[1]}
                     for pair in zip(metadata["version"], metadata["date"])],
                    key=lambda x: x["version"] if x["version"] else ""
                )

            return result
        except Exception as e:
            logger.error(f"Error processing arXivRaw record: {e}")
            return {}

    def _save_date_batch(self, metadata_format: str, date_str: str) -> None:
        """
        Save a batch of records for a specific date.

        Args:
            metadata_format: The metadata format (arXiv or arXivRaw)
            date_str: The date string in ISO format (YYYY-MM-DD)
        """
        batch = self.records_by_date[metadata_format][date_str]
        if not batch:
            return

        # Get the current sequence number for this format and date
        sequence = self.sequence_counters[metadata_format][date_str]

        # Create a filename with sequential numbering (e.g., 0001.json, 0002.json)
        filename = f"{sequence:04d}.json"

        # Convert data to JSON lines format (one JSON object per line)
        json_lines = "\n".join(json.dumps(record) for record in batch)

        if self.use_s3:
            # Save to S3 with date-based path
            try:
                object_key = f"raw/{metadata_format}/{date_str}/{filename}"
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=object_key,
                    Body=json_lines.encode('utf-8'),
                    ContentType="application/json"
                )
                logger.info(f"Saved {len(batch)} records to S3: s3://{self.bucket}/{object_key}")
            except Exception as e:
                logger.error(f"Error saving to S3: {e}")
        else:
            # Save to local file with date-based directory structure
            try:
                # Ensure directory exists
                date_dir = os.path.join(self.local_dir, "raw", metadata_format, date_str)
                os.makedirs(date_dir, exist_ok=True)

                file_path = os.path.join(date_dir, filename)
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(json_lines)
                logger.info(f"Saved {len(batch)} records to local file: {file_path}")
            except Exception as e:
                logger.error(f"Error saving to local file: {e}")

        # Increment the sequence counter for next batch
        self.sequence_counters[metadata_format][date_str] += 1

        # Clear the batch after saving
        self.records_by_date[metadata_format][date_str] = []


def main():
    """
    Main function for local execution.
    """
    # Set up command line arguments
    parser = argparse.ArgumentParser(description="Collect arXiv metadata")
    parser.add_argument("--from-date", type=str, required=True,
                        help="Start date (YYYY-MM-DD), inclusive")
    parser.add_argument("--to-date", type=str, required=True,
                        help="End date (YYYY-MM-DD), exclusive")
    parser.add_argument("--local-dir", type=str, default="data",
                        help="Local directory for storing data")
    parser.add_argument("--use-s3", action="store_true",
                        help="Use S3 for storage")
    parser.add_argument("--bucket", type=str, default="hackmd-project-2025",
                        help="S3 bucket name")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Number of records per batch")

    args = parser.parse_args()

    # Create collector
    collector = ArxivCollector(
        from_str=args.from_date,
        to_str=args.to_date,
        local_dir=args.local_dir,
        use_s3=args.use_s3,
        bucket=args.bucket,
        batch_size=args.batch_size
    )

    # Run collection
    stats = collector.collect()

    # Display results
    print("\nCollection completed!")
    print(f"Total records: {stats['total_records']}")
    print(f"Successful records: {stats['successful_records']}")
    print(f"Failed records: {stats['failed_records']}")
    print(f"Start time: {stats['start_time']}")
    print(f"End time: {stats['end_time']}")


if __name__ == "__main__":
    main()
