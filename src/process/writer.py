import logging
import time
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Any, Optional
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PersistenceWriter:
    """
    Writes processed ArXiv data to persistence layer.

    This class handles writing transformed data to storage systems,
    with support for merging updates to existing records.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the persistence writer.

        Args:
            config: Configuration dictionary with writer parameters
        """
        self.config = config
        self.region_name = config.get("region_name", "ap-northeast-1")
        self.table_name = config.get("table_name", "arxiv-papers")
        self.batch_size = min(config.get("batch_size", 25), 25)  # DynamoDB batch write limit is 25

        # Parallel processing configuration
        self.max_workers = config.get("max_workers", 5)  # Default to 5 parallel workers
        self.parallel_enabled = config.get("parallel_enabled", True)  # Enable by default

        # Initialize DynamoDB resources
        self.dynamodb = boto3.resource("dynamodb", region_name=self.region_name)
        self.table = self.dynamodb.Table(self.table_name)

        # Initialize statistics
        self.stats = {
            "total_records": 0,
            "new_records": 0,
            "updated_records": 0,
            "failed_records": 0,
            "start_time": None,
            "end_time": None,
            "processing_time": 0,
        }

        # Ensure table exists
        self._ensure_table_exists()

    def write(self, transformed_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Write transformed data to persistence layer, handling updates to existing records.
        Supports parallel processing of batches for improved performance.

        Args:
            transformed_data: List of transformed records

        Returns:
            Dictionary with write operation results
        """
        if not transformed_data:
            logger.warning("No data to write")
            return {"status": "success", "new_records": 0, "updated_records": 0}

        # Reset statistics for this write operation
        self.stats["start_time"] = datetime.now()
        self.stats["total_records"] = len(transformed_data)
        self.stats["new_records"] = 0
        self.stats["updated_records"] = 0
        self.stats["failed_records"] = 0

        try:
            start_time = time.time()

            # Prepare batches
            batches = []
            for i in range(0, len(transformed_data), self.batch_size):
                batch = transformed_data[i:i + self.batch_size]
                batches.append(batch)

            logger.info(f"Processing {len(batches)} batches with {len(transformed_data)} total records")

            if self.parallel_enabled and len(batches) > 1:
                # Process batches in parallel with limited concurrency (queue system)
                logger.info(f"Using parallel processing with max {self.max_workers} workers and controlled batch scheduling")
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Initialize tracking variables
                    active_futures = {}  # Track active futures
                    batch_queue = list(enumerate(batches))  # Queue of (index, batch) tuples
                    max_concurrent = min(5, self.max_workers)  # Limit to 5 concurrent batches
                    completed = 0
                    total_batches = len(batches)

                    # Initial submission - fill up to max_concurrent
                    while batch_queue and len(active_futures) < max_concurrent:
                        idx, batch = batch_queue.pop(0)
                        future = executor.submit(self._process_batch, batch)
                        active_futures[future] = idx

                    # Process results as they complete and submit new batches
                    while active_futures:
                        # Wait for a batch to complete
                        done, _ = concurrent.futures.wait(
                            active_futures.keys(),
                            return_when=concurrent.futures.FIRST_COMPLETED
                        )

                        # Process completed batches
                        for future in done:
                            batch_idx = active_futures.pop(future)
                            try:
                                batch_result = future.result()

                                # Update statistics
                                self.stats["new_records"] += batch_result["new_records"]
                                self.stats["updated_records"] += batch_result["updated_records"]
                                self.stats["failed_records"] += batch_result["failed_records"]

                                # Log progress
                                completed += 1
                                logger.info(f"Processed batch {batch_idx+1}/{total_batches} "
                                           f"({completed}/{total_batches} completed, {len(active_futures)} active)")

                                # Submit a new batch if available
                                if batch_queue:
                                    time.sleep(1)
                                    idx, batch = batch_queue.pop(0)
                                    future = executor.submit(self._process_batch, batch)
                                    active_futures[future] = idx
                                    logger.info(f"Scheduled new batch {idx+1}/{total_batches} "
                                          f"({len(batch_queue)} remaining in queue)")

                            except Exception as e:
                                logger.error(f"Error processing batch {batch_idx}: {e}", exc_info=True)
                                self.stats["failed_records"] += len(batches[batch_idx])

                                # Submit a new batch even if this one failed
                                if batch_queue:
                                    time.sleep(1)
                                    idx, batch = batch_queue.pop(0)
                                    future = executor.submit(self._process_batch, batch)
                                    active_futures[future] = idx
            else:
                # Process batches sequentially
                logger.info("Using sequential processing")
                for i, batch in enumerate(batches):
                    batch_result = self._process_batch(batch)

                    # Update statistics
                    self.stats["new_records"] += batch_result["new_records"]
                    self.stats["updated_records"] += batch_result["updated_records"]
                    self.stats["failed_records"] += batch_result["failed_records"]

                    # Log progress
                    logger.info(f"Processed batch {i+1}/{len(batches)} ({(i+1)*self.batch_size}/{len(transformed_data)} records)")

                    # Add a delay to avoid throttling
                    if i < len(batches) - 1:
                        time.sleep(0.5)

            # Calculate processing time
            end_time = time.time()
            self.stats["processing_time"] = round(end_time - start_time, 2)
            self.stats["end_time"] = datetime.now()

            logger.info(f"Write operation completed in {self.stats['processing_time']} seconds: "
                        f"{self.stats['new_records']} new records, "
                        f"{self.stats['updated_records']} updated records, "
                        f"{self.stats['failed_records']} failed records")

        except Exception as e:
            logger.error(f"Error writing to persistence layer: {e}", exc_info=True)
            self.stats["status"] = "error"
            self.stats["error"] = str(e)
            self.stats["end_time"] = datetime.now()

        # Return a copy of the stats
        return {
            "status": self.stats.get("status", "success"),
            "new_records": self.stats["new_records"],
            "updated_records": self.stats["updated_records"],
            "failed_records": self.stats["failed_records"],
            "processing_time": self.stats["processing_time"]
        }

    def _process_batch(self, batch: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Process a batch of records, handling updates to existing records.

        Args:
            batch: List of records to process

        Returns:
            Dictionary with batch processing statistics
        """
        result = {
            "new_records": 0,
            "updated_records": 0,
            "failed_records": 0
        }

        for record in batch:
            try:
                paper_id = record.get("paper_id")
                if not paper_id:
                    logger.warning("Record missing paper_id, skipping")
                    result["failed_records"] += 1
                    continue

                # Get existing record if any
                existing_record = self._get_existing_record(paper_id)

                if existing_record:
                    # Update existing record
                    merged_record = self._merge_records(existing_record, record)
                    self._put_record(merged_record)
                    result["updated_records"] += 1
                else:
                    # Create new record
                    self._put_record(record)
                    result["new_records"] += 1

            except Exception as e:
                logger.error(f"Error processing record {record.get('paper_id', 'unknown')}: {e}", exc_info=True)
                result["failed_records"] += 1

        # Add a small delay after processing each batch to reduce DynamoDB throughput pressure
        time.sleep(0.2)

        return result

    def _get_existing_record(self, paper_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an existing record from persistence layer.

        Args:
            paper_id: Paper ID to look up

        Returns:
            Existing record or None if not found
        """
        try:
            response = self.table.get_item(
                Key={"paper_id": paper_id}
            )

            return response.get("Item")

        except ClientError as e:
            logger.error(f"Error getting item {paper_id}: {e}")
            return None

    def _merge_records(self, existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge an existing record with a new record.

        Args:
            existing: Existing record
            new: New record

        Returns:
            Merged record
        """
        # Start with the existing record
        merged = existing.copy()

        # Update with new data, handling special cases
        for key, value in new.items():
            # Skip empty values
            if value is None or value == "":
                continue

            # Handle special merging for certain fields
            if key == "authors" and "authors" in existing:
                # Keep existing authors if new record doesn't have any
                if not value:
                    continue

            elif key == "categories" and "categories" in existing:
                # Combine categories, removing duplicates
                if isinstance(value, list) and isinstance(existing["categories"], list):
                    merged["categories"] = list(set(existing["categories"] + value))
                    continue

            elif key == "versions" and "versions" in existing:
                # Combine versions, keeping the latest
                if isinstance(value, list) and isinstance(existing["versions"], list):
                    # Get all version numbers
                    existing_versions = {v.get("version"): v for v in existing["versions"] if v.get("version")}
                    new_versions = {v.get("version"): v for v in value if v.get("version")}

                    # Merge dictionaries, new versions take precedence
                    all_versions = {**existing_versions, **new_versions}

                    # Convert back to list
                    merged["versions"] = list(all_versions.values())

                    # Update version_count
                    merged["version_count"] = len(merged["versions"])
                    continue

            # Default: new value replaces existing
            merged[key] = value

        # Always update last_processed timestamp
        merged["last_processed"] = new.get("last_processed")

        return merged

    def _put_record(self, record: Dict[str, Any]) -> None:
        """
        Put a record into persistence layer.

        Args:
            record: Record to put
        """
        # Convert Python types to DynamoDB types
        item = self._convert_types_for_dynamodb(record)

        # Put item
        self.table.put_item(Item=item)

    def _convert_types_for_dynamodb(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert Python types to DynamoDB compatible types.

        Args:
            record: Record with Python types

        Returns:
            Record with DynamoDB compatible types
        """
        # Create a new dictionary for the converted item
        item = {}

        for key, value in record.items():
            # Handle None values
            if value is None:
                continue

            # Handle lists
            elif isinstance(value, list):
                if not value:  # Skip empty lists
                    continue

                # Check if list contains dictionaries
                if value and isinstance(value[0], dict):
                    # Convert each dictionary in the list
                    item[key] = [self._convert_types_for_dynamodb(v) for v in value]
                else:
                    item[key] = value

            # Handle dictionaries
            elif isinstance(value, dict):
                item[key] = self._convert_types_for_dynamodb(value)

            # Handle boolean values (must come before int check since bool is a subclass of int)
            elif isinstance(value, bool):
                item[key] = value

            # Handle numbers (convert to Decimal for DynamoDB)
            elif isinstance(value, (int, float)):
                try:
                    item[key] = Decimal(str(value))
                except Exception as e:
                    logger.warning(f"Error converting {key} ({value}) to Decimal: {e}")
                    item[key] = value

            # Handle other types
            else:
                item[key] = value

        return item

    def _ensure_table_exists(self) -> None:
        """
        Ensure the persistence table exists, creating it if necessary.
        """
        try:
            # Check if table exists
            self.dynamodb.meta.client.describe_table(TableName=self.table_name)
            logger.info(f"Table {self.table_name} already exists")

        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                # Table doesn't exist, create it
                logger.info(f"Creating table {self.table_name}")

                table = self.dynamodb.create_table(
                    TableName=self.table_name,
                    KeySchema=[
                        {
                            "AttributeName": "paper_id",
                            "KeyType": "HASH"  # Partition key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            "AttributeName": "paper_id",
                            "AttributeType": "S"
                        },
                        {
                            "AttributeName": "primary_category",
                            "AttributeType": "S"
                        },
                        {
                            "AttributeName": "update_date",
                            "AttributeType": "S"
                        }
                    ],
                    GlobalSecondaryIndexes=[
                        {
                            "IndexName": "CategoryIndex",
                            "KeySchema": [
                                {
                                    "AttributeName": "primary_category",
                                    "KeyType": "HASH"
                                },
                                {
                                    "AttributeName": "update_date",
                                    "KeyType": "RANGE"
                                }
                            ],
                            "Projection": {
                                "ProjectionType": "ALL"
                            },
                            "ProvisionedThroughput": {
                                "ReadCapacityUnits": 5,
                                "WriteCapacityUnits": 5
                            }
                        }
                    ],
                    ProvisionedThroughput={
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5
                    }
                )

                # Wait for table creation
                table.meta.client.get_waiter("table_exists").wait(TableName=self.table_name)
                logger.info(f"Table {self.table_name} created successfully")

            else:
                # Other error
                logger.error(f"Error checking table existence: {e}")
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
