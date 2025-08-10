import logging
from typing import Dict, List, Any
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Extractor:
    """
    Responsible for extracting data from source locations using AWS Glue.

    This class handles reading data from S3 or other sources and
    performing initial filtering operations.
    """

    def __init__(self, glue_context, config: Dict[str, Any]):
        """
        Initialize the Glue data extractor.

        Args:
            glue_context: The GlueContext for the current job
            config: Configuration dictionary with extraction parameters
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.config = config

        # Extract configuration
        self.input_path = config.get("input_path", "")
        self.bucket = config.get("bucket", "")
        self.formats = config.get("formats", ["arXiv", "arXivRaw"])

        # Full S3 path
        if self.bucket:
            self.s3_input_path = f"s3://{self.bucket}/{self.input_path}"
        else:
            self.s3_input_path = self.input_path

        # Statistics
        self.stats = {
            "total_records": 0,
            "filtered_records": 0,
            "start_time": None,
            "end_time": None,
        }

    def extract(self) -> DataFrame:
        """
        Extract raw data from S3 source.

        Returns:
            DataFrame with raw data
        """
        self.stats["start_time"] = datetime.now()
        logger.info(f"Extracting data from {self.s3_input_path}")

        try:
            # Create dynamic frame from S3
            dynamic_frame = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [self.s3_input_path],
                    "recurse": True
                },
                format="json"
            )

            # Convert to DataFrame for easier processing
            df = dynamic_frame.toDF()
            self.stats["total_records"] = df.count()
            logger.info(f"Extracted {self.stats['total_records']} records")

            # No date filtering for historical data

            self.stats["filtered_records"] = df.count()
            self.stats["end_time"] = datetime.now()

            return df

        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            self.stats["end_time"] = datetime.now()
            raise

    # Removed date filtering method as it's not needed for historical data

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
