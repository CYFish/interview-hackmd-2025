import logging
import pendulum
from datetime import datetime
from typing import Dict, List, Any, Optional


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ArxivMetadataTransformer:
    """
    Transforms raw ArXiv data into a structured format.

    This class handles data cleaning, normalization, and enrichment
    of combined arXiv and arXivRaw data.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the ArXiv metadata transformer.

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
        }

    def transform(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform raw ArXiv data by combining arXiv and arXivRaw formats.

        Args:
            raw_data: List of raw records with format type indicators

        Returns:
            List of transformed records
        """
        # Group records by ID to combine arXiv and arXivRaw data
        records_by_id = {}

        # First pass: group records by ID
        for record in raw_data:
            record_id = record.get("id")
            if not record_id:
                logger.warning("Record missing ID, skipping")
                continue

            format_type = record.get("_format_type")
            if not format_type:
                logger.warning(f"Record {record_id} missing format type, skipping")
                continue

            if record_id not in records_by_id:
                records_by_id[record_id] = {"arXiv": None, "arXivRaw": None, "update_date": None}

            records_by_id[record_id][format_type] = record

            # Store update_date if available
            if "update_date" in record:
                records_by_id[record_id]["update_date"] = record["update_date"]

        # Process combined records in batches
        self.stats["start_time"] = datetime.now()
        self.stats["input_records"] = len(records_by_id)

        transformed_records = []
        ids = list(records_by_id.keys())

        for i in range(0, len(ids), self.batch_size):
            batch_ids = ids[i:i + self.batch_size]
            logger.info(f"Transforming batch {i//self.batch_size + 1}, size: {len(batch_ids)}")
            batch_failures = 0

            for record_id in batch_ids:
                try:
                    record_data = records_by_id[record_id]
                    arxiv_record = record_data["arXiv"]
                    arxiv_raw_record = record_data["arXivRaw"]

                    # Skip if we don't have both formats
                    if not arxiv_record or not arxiv_raw_record:
                        logger.warning(f"Record {record_id} missing one format, skipping")
                        batch_failures += 1
                        continue

                    # Clean the combined record
                    cleaned = self._clean_record(arxiv_record, arxiv_raw_record)
                    if cleaned:
                        # Calculate derived metrics
                        cleaned["update_frequency"] = self._calculate_update_frequency(
                            arxiv_raw_record)
                        cleaned["submission_to_publication"] = self._calculate_submission_to_publication(
                            cleaned["submitted_date"],
                            cleaned["published_date"]
                        )

                        transformed_records.append(cleaned)
                    else:
                        # Record cleaning failed
                        batch_failures += 1

                except Exception as e:
                    logger.error(f"Error transforming record {record_id}: {e}", exc_info=True)
                    batch_failures += 1

            # Update statistics
            self.stats["failed_records"] += batch_failures
            logger.info(f"Batch {i//self.batch_size + 1} completed: {len(batch_ids) - batch_failures} successful, {batch_failures} failed")

        # Update final statistics
        self.stats["output_records"] = len(transformed_records)
        self.stats["end_time"] = datetime.now()

        logger.info(f"Transformed {self.stats['output_records']} records successfully, {self.stats['failed_records']} records failed")
        return transformed_records

    def _clean_record(self, arxiv_record: Dict[str, Any], arxiv_raw_record: Dict[str, Any], update_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Clean and normalize a single ArXiv record.

        Args:
            arxiv_record: arXiv format record
            arxiv_raw_record: arXivRaw format record

        Returns:
            Cleaned record or None if record should be skipped
        """
        try:
            # Extract paper ID
            paper_id = arxiv_record.get("id")
            if not paper_id:
                logger.warning("Record missing ID, skipping")
                return None

            # Build base item with core fields
            item = {
                "paper_id": paper_id,
                "title": self._clean_text(arxiv_record.get("title", "")),
                "abstract": self._clean_text(arxiv_record.get("abstract", "")),
                "doi": arxiv_record.get("doi", ""),
                "upload_db_time": pendulum.now(self.time_zone).isoformat()
            }

            # Extract categories
            item["categories"] = arxiv_record.get("categories", "").split() if arxiv_record.get("categories") else []
            item["primary_category"] = item["categories"][0] if item["categories"] else ""

            # Extract publication information
            item["journal_ref"] = arxiv_record.get("journal_ref", "")
            item["is_published"] = bool(item["journal_ref"])

            # Process author information
            if "authors_parsed" in arxiv_record and isinstance(arxiv_record["authors_parsed"], list):
                authors = []
                for author_parts in arxiv_record["authors_parsed"]:
                    if isinstance(author_parts, list) and len(author_parts) >= 2:
                        last_name, first_name = author_parts[0], author_parts[1]
                        authors.append({
                            "last_name": last_name,
                            "first_name": first_name
                        })
                item["authors"] = authors

            # Extract institution from submitter email if available
            if "submitter" in arxiv_raw_record and "@" in arxiv_raw_record["submitter"]:
                try:
                    email = arxiv_raw_record["submitter"].split("<")[1].split(">")[0]
                    domain = email.split("@")[1]
                    institution = domain.split(".")[0]
                    item["institution"] = institution
                except (IndexError, AttributeError):
                    item["institution"] = ""
            else:
                item["institution"] = ""

            # Extract version information
            if "versions" in arxiv_raw_record and isinstance(arxiv_raw_record["versions"], list):
                item["versions"] = arxiv_raw_record["versions"]
                item["version_count"] = len(arxiv_raw_record["versions"])

                # Parse submission date from the first version
                version_dates = []
                try:
                    version_dates = [
                        pendulum.from_format(version["created"], "ddd, D MMM YYYY HH:mm:ss z").in_timezone(self.time_zone)
                        for version in arxiv_raw_record["versions"]
                    ]
                    version_dates.sort()
                except Exception as e:
                    logger.warning(f"Error parsing submission date: {e}")
                    item["submitted_date"] = None

                item["submitted_date"] = version_dates[0].date().isoformat()

                item["published_date"] = version_dates[-1].date().isoformat() if item["is_published"] else None

            # Extract update_date
            item["update_date"] = None
            for source, field in [
                (arxiv_record, "update_date"),
                (arxiv_record, "datestamp"),
                (arxiv_raw_record, "datestamp")
            ]:
                if field in source:
                    try:
                        item["update_date"] = pendulum.parse(source[field]).date().isoformat()
                        break  # Successfully parsed a date, no need to check other fields
                    except Exception as e:
                        logger.warning(f"Error parsing {field} from {source.get('id', 'unknown')}: {e}")
                        # Continue to next possible field

            return item

        except Exception as e:
            logger.error(f"Error transforming record {arxiv_record.get('id', 'unknown')}: {e}", exc_info=True)
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

    def _calculate_update_frequency(self, arxiv_raw_record: Dict[str, Any]) -> Optional[float]:
        """
        Calculate the average update frequency in days.

        Args:
            arxiv_raw_record: ArXivRaw metadata record

        Returns:
            Average days between updates or None if not applicable
        """
        versions = arxiv_raw_record.get("versions", [])
        if len(versions) <= 1:
            return None

        try:
            dates = []
            for version in versions:
                date = pendulum.from_format(version["created"], "ddd, D MMM YYYY HH:mm:ss z")
                dates.append(date)

            dates.sort()
            total_days = (dates[-1] - dates[0]).total_days()
            return total_days / (len(dates) - 1)
        except Exception:
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
            except Exception:
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
