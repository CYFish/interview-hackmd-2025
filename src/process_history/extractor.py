import json
import logging
from typing import Dict, List, Any

import boto3


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Extractor:
    """
    Extracts ArXiv historical data from source systems.

    This class is responsible for retrieving raw ArXiv historical metadata from a JSON file.
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
        self.input_local = config.get("input_local", False)
        self.input_path = config.get("input_path")

        # Initialize S3 client if we need to read from S3
        if not self.input_local:
            self.s3 = boto3.client("s3", region_name=self.region_name)

        # Initialize statistics
        self.stats = {
            "total_records": 0,
            "processed_files": 0,
            "failed_files": 0,
            "start_time": None,
            "end_time": None,
        }

    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract all data from the historical ArXiv JSON file at once.

        Note: This method loads the entire dataset into memory.
        For large files, consider using extract_stream() instead.

        Returns:
            List of raw records
        """
        from datetime import datetime
        self.stats["start_time"] = datetime.now()

        try:
            logger.info(f"Extracting all data from {'local file' if self.input_local else 'S3'}: {self.input_path}")

            data = []
            if self.input_local:
                # Read from local file system
                with open(self.input_path, "r", encoding="utf-8") as file:
                    for line in file:
                        if line.strip():  # Skip empty lines
                            try:
                                record = json.loads(line.strip())
                                data.append(record)
                            except json.JSONDecodeError as e:
                                logger.warning(f"Error parsing JSON line: {e}. Skipping line.")
            else:
                # Read from S3
                file_object = self.s3.get_object(Bucket=self.s3_bucket, Key=self.input_path)
                lines = file_object["Body"].read().decode("utf-8").splitlines()
                for line in lines:
                    if line.strip():  # Skip empty lines
                        try:
                            record = json.loads(line.strip())
                            data.append(record)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Error parsing JSON line: {e}. Skipping line.")

            # Ensure data is a list
            if not isinstance(data, list):
                logger.warning(f"Input data is not a list. Converting single record to list.")
                data = [data]

            # Update statistics
            self.stats["total_records"] = len(data)
            self.stats["processed_files"] = 1

            logger.info(f"Successfully extracted {len(data)} records from historical data file")
            self.stats["end_time"] = datetime.now()
            return data
        except Exception as e:
            logger.error(f"Error extracting data from {self.input_path}: {e}", exc_info=True)
            self.stats["failed_files"] = 1
            self.stats["end_time"] = datetime.now()
            return []

    def extract_stream(self):
        """
        Stream data from the historical ArXiv JSON file one record at a time.

        This generator function yields records one by one without loading the entire
        dataset into memory, making it suitable for processing large files.

        Yields:
            Dict[str, Any]: Individual JSON records
        """
        from datetime import datetime
        self.stats["start_time"] = datetime.now()
        record_count = 0

        try:
            logger.info(f"Streaming data from {'local file' if self.input_local else 'S3'}: {self.input_path}")

            if self.input_local:
                # Stream from local file system
                with open(self.input_path, "r", encoding="utf-8") as file:
                    for i, line in enumerate(file):
                        if line.strip():  # Skip empty lines
                            try:
                                record = json.loads(line.strip())
                                record_count += 1
                                if record_count % 1000 == 0:
                                    logger.info(f"Streamed {record_count} records so far")
                                yield record
                            except json.JSONDecodeError as e:
                                logger.warning(f"Error parsing JSON at line {i + 1}: {e}")
            else:
                # Stream from S3
                file_object = self.s3.get_object(Bucket=self.s3_bucket, Key=self.input_path)
                # Use boto3's streaming capabilities
                for i, line in enumerate(file_object["Body"].iter_lines()):
                    if line:  # Skip empty lines
                        try:
                            record = json.loads(line.decode("utf-8"))
                            record_count += 1
                            if record_count % 1000 == 0:
                                logger.info(f"Streamed {record_count} records so far")
                            yield record
                        except json.JSONDecodeError as e:
                            logger.warning(f"Error parsing JSON at line {i + 1}: {e}")

            # Update statistics
            self.stats["total_records"] = record_count
            self.stats["processed_files"] = 1
            logger.info(f"Successfully streamed {record_count} records from historical data file")

        except Exception as e:
            logger.error(f"Error streaming data from {self.input_path}: {e}", exc_info=True)
            self.stats["failed_files"] = 1
        finally:
            self.stats["end_time"] = datetime.now()

    def get_stats(self) -> Dict[str, Any]:
        """
        Get extraction statistics.

        Returns:
            Dictionary with extraction statistics
        """
        if self.stats["start_time"] and self.stats["end_time"]:
            duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            self.stats["duration_seconds"] = duration

        return self.stats
