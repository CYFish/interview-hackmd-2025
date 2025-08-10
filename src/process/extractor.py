import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

import boto3


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataSourceExtractor:
    """
    Extracts ArXiv data from source systems.

    This class is responsible for retrieving raw ArXiv metadata from various sources,
    supporting both arXiv and arXivRaw formats.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the data source extractor.

        Args:
            config: Configuration dictionary with extraction parameters
        """
        self.config = config
        self.region_name = config.get("region_name", "ap-northeast-1")
        self.s3_bucket = config.get("s3_bucket", "hackmd-project-2025")
        self.local_mode = config.get("local_mode", False)
        self.local_dir = config.get("local_dir", "data")

        # Initialize S3 client if not in local mode
        if not self.local_mode:
            self.s3 = boto3.client("s3", region_name=self.region_name)

    def extract_date_range(self, from_date: str, to_date: str) -> List[Dict[str, Any]]:
        """
        Extract data for a specific date range, combining both arXiv and arXivRaw formats.

        Args:
            from_date: Start date in ISO format (YYYY-MM-DD), inclusive
            to_date: End date in ISO format (YYYY-MM-DD), exclusive

        Returns:
            List of raw records
        """
        all_records = []

        # Process both formats
        for format_type in ["arXiv", "arXivRaw"]:
            format_records = self.extract_format_type(format_type, from_date, to_date)
            all_records.extend(format_records)
            logger.info(f"Extracted {len(format_records)} {format_type} records")

        logger.info(f"Total extracted records: {len(all_records)}")
        return all_records

    def extract_format_type(self, format_type: str, from_date: str, to_date: str) -> List[Dict[str, Any]]:
        """
        Extract data for a specific format type and date range.

        Args:
            format_type: Format type ("arXiv" or "arXivRaw")
            from_date: Start date in ISO format (YYYY-MM-DD), inclusive
            to_date: End date in ISO format (YYYY-MM-DD), exclusive

        Returns:
            List of raw records
        """
        all_records = []

        try:
            # Convert date strings to datetime objects
            start_date = datetime.strptime(from_date, "%Y-%m-%d")
            end_date = datetime.strptime(to_date, "%Y-%m-%d")

            # Iterate through each date in the range
            current_date = start_date
            while current_date < end_date:
                date_str = current_date.strftime("%Y-%m-%d")

                # Extract data for this date
                date_records = self._extract_date(format_type, date_str)
                all_records.extend(date_records)

                # Move to the next day
                current_date += timedelta(days=1)

        except Exception as e:
            logger.error(f"Error extracting {format_type} data for date range {from_date} to {to_date}: {e}",
                         exc_info=True)

        return all_records

    def _extract_date(self, format_type: str, date_str: str) -> List[Dict[str, Any]]:
        """
        Extract data for a specific format type and date.

        Args:
            format_type: Format type ("arXiv" or "arXivRaw")
            date_str: Date string in ISO format (YYYY-MM-DD)

        Returns:
            List of raw records
        """
        records = []

        try:
            if self.local_mode:
                records = self._extract_local(format_type, date_str)
            else:
                records = self._extract_s3(format_type, date_str)

            logger.info(f"Extracted {len(records)} {format_type} records for {date_str}")

        except Exception as e:
            logger.error(f"Error extracting {format_type} data for {date_str}: {e}", exc_info=True)

        return records

    def _extract_s3(self, format_type: str, date_str: str) -> List[Dict[str, Any]]:
        """
        Extract data from S3 for a specific format type and date.

        Args:
            format_type: Format type ("arXiv" or "arXivRaw")
            date_str: Date string in ISO format (YYYY-MM-DD)

        Returns:
            List of raw records
        """
        records = []

        # Construct S3 prefix
        prefix = f"raw/{format_type}/{date_str}/"

        try:
            # List objects with the prefix
            response = self.s3.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=prefix
            )

            # If no objects found, return empty list
            if "Contents" not in response:
                logger.info(f"No {format_type} data found for {date_str}")
                return records

            # Process each object
            for obj in response["Contents"]:
                key = obj["Key"]

                # Skip if not a JSON file
                if not key.endswith(".json"):
                    continue

                # Get object content
                obj_response = self.s3.get_object(
                    Bucket=self.s3_bucket,
                    Key=key
                )

                # Read content
                content = obj_response["Body"].read().decode("utf-8")

                # Parse JSON (each line is a separate JSON object)
                for line in content.strip().split("\n"):
                    try:
                        record = json.loads(line)
                        # Add format_type to the record for later processing
                        record["_format_type"] = format_type
                        records.append(record)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Error parsing JSON line in {key}: {e}")

        except Exception as e:
            logger.error(f"Error extracting {format_type} data from S3 for {date_str}: {e}", exc_info=True)

        return records

    def _extract_local(self, format_type: str, date_str: str) -> List[Dict[str, Any]]:
        """
        Extract data from local file system for a specific format type and date.

        Args:
            format_type: Format type ("arXiv" or "arXivRaw")
            date_str: Date string in ISO format (YYYY-MM-DD)

        Returns:
            List of raw records
        """
        import os
        records = []

        # Construct local directory path
        dir_path = os.path.join(self.local_dir, "raw", format_type, date_str)

        try:
            # Check if directory exists
            if not os.path.exists(dir_path):
                logger.info(f"No {format_type} data found for {date_str} in local directory")
                return records

            # List JSON files in the directory
            json_files = [f for f in os.listdir(dir_path) if f.endswith(".json")]

            # Process each file
            for json_file in json_files:
                file_path = os.path.join(dir_path, json_file)

                # Read file content
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                # Parse JSON (each line is a separate JSON object)
                for line in content.strip().split("\n"):
                    try:
                        record = json.loads(line)
                        # Add format_type to the record for later processing
                        record["_format_type"] = format_type
                        records.append(record)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Error parsing JSON line in {file_path}: {e}")

        except Exception as e:
            logger.error(f"Error extracting {format_type} data from local directory for {date_str}: {e}",
                         exc_info=True)

        return records
