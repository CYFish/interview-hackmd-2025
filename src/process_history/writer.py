import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Writer:
    """
    Writes processed ArXiv historical data to storage.

    This class handles writing transformed data to Parquet files,
    with support for partitioning by date.
    """

    # Default column order for Parquet files
    DEFAULT_COLUMN_ORDER = [
        "paper_id",
        "is_published",
        "title",
        "abstract",
        "doi",
        "journal_ref",
        "institution",
        "categories",
        "primary_category",
        "submitted_date",
        "published_date",
        "update_date",
        "authors",
        "versions",
        "version_count",
        "update_frequency",
        "submission_to_publication",
        "upload_db_time",
        "year",
        "month",
        "day"
    ]

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the persistence writer.

        Args:
            config: Configuration dictionary with writer parameters
        """
        self.config = config
        self.region_name = config.get("region_name", "ap-northeast-1")
        self.s3_bucket = config.get("s3_bucket", "hackmd-project-2025")
        self.output_local = config.get("output_local", False)
        self.output_path = config.get("output_path")
        self.batch_size = config.get("batch_size", 1000)

        # Initialize S3 client if we need to write to S3
        if not self.output_local:
            self.s3 = boto3.client("s3", region_name=self.region_name)

        # Initialize statistics
        self.stats = {
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "start_time": None,
            "end_time": None,
            "processing_time": 0,
        }

        # Initialize counters for file naming
        self.counters = {}

    def write(self, transformed_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Write transformed data to Parquet files, partitioned by date.

        Args:
            transformed_data: List of transformed records

        Returns:
            Dictionary with write operation results
        """
        if not transformed_data:
            logger.warning("No data to write")
            return {"status": "success", "records_written": 0}

        # Reset statistics for this write operation
        self.stats["start_time"] = datetime.now()
        self.stats["total_records"] = len(transformed_data)
        self.stats["successful_records"] = 0
        self.stats["failed_records"] = 0

        try:
            start_time = time.time()

            # Group records by partition
            partitioned_records = {}
            for record in transformed_data:
                partition_key = f"year={record['year']}/month={record['month']}/day={record['day']}"
                if partition_key not in partitioned_records:
                    partitioned_records[partition_key] = []
                partitioned_records[partition_key].append(record)

            # Write each partition
            for partition_key, records in partitioned_records.items():
                if records:
                    try:
                        self._write_parquet(partition_key, records)
                        self.stats["successful_records"] += len(records)
                    except Exception as e:
                        logger.error(f"Error writing partition {partition_key}: {e}", exc_info=True)
                        self.stats["failed_records"] += len(records)

            # Calculate processing time
            end_time = time.time()
            self.stats["processing_time"] = round(end_time - start_time, 2)
            self.stats["end_time"] = datetime.now()

            logger.info(f"Write operation completed in {self.stats['processing_time']} seconds: "
                        f"{self.stats['successful_records']} records written, "
                        f"{self.stats['failed_records']} records failed")

        except Exception as e:
            logger.error(f"Error writing to storage: {e}", exc_info=True)
            self.stats["status"] = "error"
            self.stats["error"] = str(e)
            self.stats["end_time"] = datetime.now()

        # Return a copy of the stats
        return {
            "status": self.stats.get("status", "success"),
            "records_written": self.stats["successful_records"],
            "failed_records": self.stats["failed_records"],
            "processing_time": self.stats["processing_time"]
        }

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

            # Handle nested lists and dicts for authors
            if "authors" in df.columns:
                # Ensure authors is properly formatted for PyArrow
                df["authors"] = df["authors"].apply(
                    lambda x: x if isinstance(x, list) else []
                )

            # Update counter
            if partition_key not in self.counters:
                self.counters[partition_key] = 1
            else:
                self.counters[partition_key] += 1

            table = pa.Table.from_pandas(df[self.DEFAULT_COLUMN_ORDER])

            if self.output_local:
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
                        Bucket=self.s3_bucket,
                        Key=object_key,
                        Body=data.to_pybytes()
                    )

                logger.info(f"✓ Wrote {len(batch)} records to s3://{self.s3_bucket}/{object_key}")

        except Exception as e:
            logger.error(f"Error writing Parquet file: {e}", exc_info=True)
            raise

    def get_stats(self) -> Dict[str, Any]:
        """
        Get write operation statistics.

        Returns:
            Dictionary with write operation statistics
        """
        if self.stats["start_time"] and self.stats["end_time"]:
            duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            self.stats["duration_seconds"] = duration

        return self.stats
