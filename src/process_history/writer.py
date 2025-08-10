import logging
from typing import Dict, List, Any
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, year, month, dayofmonth, to_date

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Writer:
    """
    Responsible for writing processed historical data to storage using AWS Glue.

    This class handles writing transformed historical data to S3 in Parquet format,
    with partitioning and deduplication. It uses overwrite mode to ensure clean data.
    """

    def __init__(self, glue_context, config: Dict[str, Any]):
        """
        Initialize the Glue data writer.

        Args:
            glue_context: The GlueContext for the current job
            config: Configuration dictionary with writer parameters
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.config = config

        # Extract configuration
        self.output_path = config.get("output_path", "")
        self.bucket = config.get("bucket", "")
        self.partition_by = config.get("partition_by", ["year", "month", "day"])
        self.database_name = config.get("database_name", "arxiv_history")
        self.table_name = config.get("table_name", "processed_papers")

        # Full S3 path
        if self.bucket:
            self.s3_output_path = f"s3://{self.bucket}/{self.output_path}"
        else:
            self.s3_output_path = self.output_path

        # Statistics tracking
        self.stats = {
            "records_written": 0,
            "start_time": None,
            "end_time": None,
            "duration_seconds": 0
        }

    def write(self, df: DataFrame) -> Dict[str, Any]:
        """
        Write transformed historical data to S3 using Glue.

        Args:
            df: DataFrame with transformed historical data

        Returns:
            Dictionary with write operation results including record count
        """
        self.stats["start_time"] = datetime.now()

        if df.count() == 0:
            logger.warning("No data to write")
            self.stats["end_time"] = datetime.now()
            return {"status": "success", "records_written": 0}

        try:
            # Track record count before processing
            initial_count = df.count()
            logger.info(f"Processing {initial_count} records for writing")

            # Prepare DataFrame for writing by ensuring partition columns exist
            df = self._prepare_dataframe_for_writing(df)

            # Deduplicate records by ID if needed
            if 'id' in df.columns and initial_count > 1:
                df = df.orderBy(col("update_date").desc())
                df = df.dropDuplicates(["id"])
                dedup_count = df.count()
                logger.info(f"Removed {initial_count - dedup_count} duplicate records, {dedup_count} unique records remain")

            # Write data to S3
            result = self._write_to_s3(df)

            self.stats["records_written"] = result.get("records_written", 0)
            self.stats["end_time"] = datetime.now()

            return result

        except Exception as e:
            self.stats["end_time"] = datetime.now()
            self.stats["error"] = str(e)

            error_msg = f"Data writing failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            return {
                "status": "error",
                "error": str(e),
                "records_attempted": initial_count,
                "records_written": 0
            }

    def _prepare_dataframe_for_writing(self, df: DataFrame) -> DataFrame:
        """
        Prepare the DataFrame for writing by ensuring partition columns exist.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with partition columns
        """
        # Ensure date columns are properly formatted
        if "submitted_date" in df.columns:
            df = df.withColumn("submitted_date", to_date(col("submitted_date")))

        # Add partition columns if they don't exist
        if "year" not in df.columns and "submitted_date" in df.columns:
            df = df.withColumn("year", year(col("submitted_date")).cast("string"))

        if "month" not in df.columns and "submitted_date" in df.columns:
            df = df.withColumn("month", month(col("submitted_date")).cast("string").cast("int").cast("string").rpad(2, "0"))

        if "day" not in df.columns and "submitted_date" in df.columns:
            df = df.withColumn("day", dayofmonth(col("submitted_date")).cast("string").cast("int").cast("string").rpad(2, "0"))

        return df

    def _write_to_s3(self, df: DataFrame) -> Dict[str, Any]:
        """
        Write data to S3 in overwrite mode.

        Args:
            df: DataFrame with historical data to write

        Returns:
            Dictionary with write operation results including record count
        """
        try:
            # Convert Spark DataFrame to Glue DynamicFrame
            dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, "historical_data_frame")
            record_count = dynamic_frame.count()

            # Purge existing table if it exists
            self._purge_existing_table()

            # Write the new data
            self._write_dynamic_frame(dynamic_frame)

            logger.info(f"Successfully wrote {record_count} records to {self.s3_output_path}")

            return {
                "status": "success",
                "records_written": record_count
            }



        except Exception as e:
            error_msg = f"Failed to write data to S3 at {self.s3_output_path}: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _purge_existing_table(self) -> None:
        """
        Purge existing table if it exists in the catalog.
        """
        try:
            self.glue_context.purge_table(
                database=self.database_name,
                table_name=self.table_name
            )
            logger.info(f"Purged existing table {self.database_name}.{self.table_name}")
        except Exception:
            logger.info(f"Table {self.database_name}.{self.table_name} does not exist yet")

    def _write_dynamic_frame(self, dynamic_frame: DynamicFrame) -> None:
        """
        Write a DynamicFrame to S3 and catalog it.

        Args:
            dynamic_frame: The DynamicFrame to write
        """
        # Write to S3 with partitioning
        sink = self.glue_context.getSink(
            path=self.s3_output_path,
            connection_type="s3",
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=self.partition_by,
            compression="snappy",
            enableUpdateCatalog=True,
            transformation_ctx="write_historical_data"
        )

        sink.setCatalogInfo(
            catalogDatabase=self.database_name,
            catalogTableName=self.table_name
        )

        sink.setFormat("glueparquet")
        sink.writeFrame(dynamic_frame)

    def get_stats(self) -> Dict[str, Any]:
        """
        Get writer statistics.

        Returns:
            Dictionary with writer statistics including duration and record counts
        """
        if self.stats["start_time"] and self.stats["end_time"]:
            self.stats["duration_seconds"] = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            logger.info(f"Writing completed in {self.stats['duration_seconds']:.2f} seconds")

        return self.stats
