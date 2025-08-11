import logging
import re
import pendulum
from datetime import datetime
from typing import Dict, List, Any, Optional


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Transformer:
    """
    Transforms raw ArXiv historical data into a structured format.

    This class handles data cleaning, normalization, and enrichment
    of historical ArXiv data.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the ArXiv historical data transformer.

        Args:
            config: Configuration dictionary with transformation parameters
        """
        self.config = config
        self.batch_size = config.get("batch_size", 1000)
        self.time_zone = "UTC"

        # Initialize statistics
        self.stats = {
            "input_records": 0,
            "output_records": 0,
            "failed_records": 0,
            "start_time": None,
            "end_time": None,
            "data_quality": {
                "missing_titles": 0,
                "missing_abstracts": 0,
                "missing_categories": 0,
                "missing_authors": 0,
                "anomalies": []
            }
        }

    def transform(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform raw ArXiv historical data into a structured format.

        Args:
            raw_data: List of raw records from the historical data file

        Returns:
            List of transformed records
        """
        self.stats["start_time"] = datetime.now()
        self.stats["input_records"] = len(raw_data)

        transformed_records = []

        # Process records in batches
        for i in range(0, len(raw_data), self.batch_size):
            batch = raw_data[i:i+self.batch_size]
            logger.info(f"Transforming batch {i//self.batch_size + 1}, size: {len(batch)}")
            batch_failures = 0

            for record in batch:
                try:
                    # Clean and transform the record
                    cleaned = self._clean_record(record)
                    if cleaned:
                        transformed_records.append(cleaned)
                    else:
                        batch_failures += 1
                except Exception as e:
                    logger.error(f"Error transforming record {record.get('id', 'unknown')}: {e}", exc_info=True)
                    batch_failures += 1

            # Update statistics
            self.stats["failed_records"] += batch_failures
            logger.info(f"Batch {i//self.batch_size + 1} completed: {len(batch) - batch_failures} successful, {batch_failures} failed")

        # Update final statistics
        self.stats["output_records"] = len(transformed_records)
        self.stats["end_time"] = datetime.now()

        logger.info(f"Transformed {self.stats['output_records']} records successfully, {self.stats['failed_records']} records failed")
        return transformed_records

    def _clean_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Clean and normalize a single ArXiv record.

        Args:
            record: Raw ArXiv record from historical data

        Returns:
            Cleaned record or None if record should be skipped
        """
        try:
            # Extract paper ID
            paper_id = record.get("id")
            if not paper_id:
                logger.warning("Record missing ID, skipping")
                return None

            # Build base item with core fields
            item = {
                "paper_id": paper_id,
                "title": self._clean_text(record.get("title", "")),
                "abstract": self._clean_text(record.get("abstract", "")),
                "doi": record.get("doi", ""),
                "upload_db_time": pendulum.now(self.time_zone).isoformat()
            }

            # Track data quality
            if not item["title"]:
                self.stats["data_quality"]["missing_titles"] += 1
            if not item["abstract"]:
                self.stats["data_quality"]["missing_abstracts"] += 1

            # Extract categories
            categories = record.get("categories", "")
            if not categories:
                self.stats["data_quality"]["missing_categories"] += 1
                item["categories"] = []
            else:
                # Split categories into a list
                item["categories"] = categories.split() if isinstance(categories, str) else []

            # Set primary category as the first category
            item["primary_category"] = item["categories"][0] if item["categories"] else ""

            # Extract publication information
            item["journal_ref"] = record.get("journal-ref", "")
            item["is_published"] = bool(item["journal_ref"])

            # Process author information
            if "authors_parsed" in record and isinstance(record["authors_parsed"], list):
                authors = []
                for author_parts in record["authors_parsed"]:
                    if isinstance(author_parts, list) and len(author_parts) >= 2:
                        last_name, first_name = author_parts[0], author_parts[1]
                        print(f"last_name: {last_name} (type: {type(last_name)}), first_name: {first_name} (type: {type(first_name)})")
                        authors.append({
                            "last_name": last_name,
                            "first_name": first_name
                        })
                item["authors"] = authors
            else:
                self.stats["data_quality"]["missing_authors"] += 1
                item["authors"] = []

            # Extract institution from submitter email if available
            item["institution"] = ""
            if "submitter" in record and isinstance(record["submitter"], str):
                try:
                    if "@" in record["submitter"]:
                        # Try to extract email domain as institution
                        email_parts = record["submitter"].split("@")
                        if len(email_parts) > 1:
                            domain = email_parts[1].split(".")[0]
                            item["institution"] = domain
                except Exception as e:
                    logger.debug(f"Could not extract institution: {e}")

            # Process versions and dates
            item["version_count"] = 0
            item["submitted_date"] = None
            item["update_date"] = None
            item["published_date"] = None

            # Extract version information
            if "versions" in record and isinstance(record["versions"], list):
                item["versions"] = record["versions"]
                item["version_count"] = len(record["versions"])

                # Parse submission date from the first version
                version_dates = []
                try:
                    version_dates = [
                        pendulum.from_format(
                            version["created"], "ddd, D MMM YYYY HH:mm:ss z").in_timezone(self.time_zone)
                        for version in record["versions"]
                    ]
                    version_dates.sort()
                except Exception as e:
                    logger.warning(f"Error parsing submission date: {e}")
                    item["submitted_date"] = None

                item["submitted_date"] = version_dates[0].date().isoformat()

                item["published_date"] = version_dates[-1].date(
                ).isoformat() if item["is_published"] else None

            # Extract update_date
            try:
                item["update_date"] = pendulum.parse(record.get("update_date", None)).date().isoformat()
            except Exception as e:
                logger.warning(f"Error parsing update_date: {e}")
                item["update_date"] = None

            # Check for critical missing data
            if not item["submitted_date"]:
                # Use current date as last resort
                today = pendulum.now().date().isoformat()
                item["submitted_date"] = today

            if not item["update_date"]:
                item["update_date"] = item["submitted_date"]

            if not item["version_count"]:
                item["version_count"] = 1  # Default to 1

            # Calculate derived metrics
            item["update_frequency"] = self._calculate_update_frequency(record)
            item["submission_to_publication"] = self._calculate_submission_to_publication(
                item["submitted_date"],
                item["published_date"]
            )

            # Create partition columns based on submitted date
            try:
                submitted_date = pendulum.parse(item["submitted_date"])
                item["year"] = str(submitted_date.year)
                item["month"] = f"{submitted_date.month:02}"
                item["day"] = f"{submitted_date.day:02}"
            except Exception:
                item["year"] = "unknown"
                item["month"] = "00"
                item["day"] = "00"

            return item

        except Exception as e:
            logger.error(f"Error transforming record {record.get('id', 'unknown')}: {e}", exc_info=True)
            return None

    def _clean_text(self, text: str) -> str:
        """
        Clean and normalize text fields.

        Args:
            text: Raw text string

        Returns:
            Cleaned text string
        """
        if not text:
            return ""

        # Remove extra whitespace
        cleaned = " ".join(text.split())
        return cleaned

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

    def _calculate_update_frequency(self, record: Dict[str, Any]) -> Optional[float]:
        """
        Calculate the average update frequency in days.

        Args:
            record: ArXiv metadata record

        Returns:
            Average days between updates or None if not applicable
        """
        versions = record.get("versions", [])
        if len(versions) <= 1:
            return None

        try:
            dates = []
            for version in versions:
                if "created" in version:
                    date = self._parse_arxiv_date(version["created"])
                    if date:
                        dates.append(date)

            if len(dates) <= 1:
                return None

            dates.sort()
            total_days = (dates[-1] - dates[0]).total_days()
            return total_days / (len(dates) - 1)
        except Exception as e:
            logger.debug(f"Could not calculate update frequency: {e}")
            return None

    def _calculate_submission_to_publication(
        self,
        submitted_date: Optional[str],
        published_date: Optional[str]
    ) -> Optional[float]:
        """
        Calculate days from submission to publication.

        Args:
            submitted_date: Date of first submission
            published_date: Date of publication

        Returns:
            Number of days or None if not applicable
        """
        if submitted_date and published_date:
            try:
                submit_date = pendulum.parse(submitted_date)
                publish_date = pendulum.parse(published_date)
                delta = publish_date - submit_date
                return delta.total_days()
            except Exception as e:
                logger.debug(f"Could not calculate submission to publication time: {e}")
                return None
        return None

    def get_stats(self) -> Dict[str, Any]:
        """
        Get transformation statistics.

        Returns:
            Dictionary with transformation statistics
        """
        if self.stats["start_time"] and self.stats["end_time"]:
            duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            self.stats["duration_seconds"] = duration

        return self.stats
