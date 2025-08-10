import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

from extractor import DataSourceExtractor
from transformer import ArxivMetadataTransformer
from writer import PersistenceWriter


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataProcessingPipeline:
    """
    Main pipeline orchestrator for ArXiv metadata processing.

    This class coordinates the entire data processing workflow:
    1. Data extraction from source systems
    2. Metadata transformation and enrichment
    3. Writing processed data to persistence layer with merge functionality
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the pipeline with configuration.

        Args:
            config: Configuration dictionary containing processing parameters
        """
        self.config = config
        self.extractor = DataSourceExtractor(config)
        self.transformer = ArxivMetadataTransformer(config)
        self.writer = PersistenceWriter(config)

        # Initialize statistics
        self.stats = {
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "updated_records": 0,
            "new_records": 0,
            "start_time": None,
            "end_time": None,
            "processed_files": 0,
        }

    def process_date_range(self, from_date: str, to_date: str) -> Dict[str, Any]:
        """
        Process data for a specific date range.

        Args:
            from_date: Start date in ISO format (YYYY-MM-DD), inclusive
            to_date: End date in ISO format (YYYY-MM-DD), exclusive

        Returns:
            Dictionary with processing statistics
        """
        self.stats["start_time"] = datetime.now().isoformat()

        try:
            logger.info(f"Starting ArXiv data processing pipeline for date range {from_date} to {to_date}")

            # Step 1: Extract raw data
            logger.info("Extracting data from source")
            start_time = time.time()
            raw_data = self.extractor.extract_date_range(from_date, to_date)
            extract_time = time.time() - start_time
            logger.info(f"Extraction completed in {extract_time:.2f} seconds")

            self.stats["total_records"] = len(raw_data)
            if not raw_data:
                logger.info("No data found for the specified date range")
                self.stats["end_time"] = datetime.now().isoformat()
                return self.stats

            # Step 2: Transform data
            logger.info("Transforming metadata")
            start_time = time.time()
            transformed_data = self.transformer.transform(raw_data)
            transform_time = time.time() - start_time
            logger.info(f"Transformation completed in {transform_time:.2f} seconds")

            self.stats["successful_records"] = len(transformed_data)
            self.stats["failed_records"] = self.stats["total_records"] - self.stats["successful_records"]

            # Step 3: Write to persistence layer
            logger.info("Writing data to persistence layer")
            start_time = time.time()
            write_result = self.writer.write(transformed_data)
            write_time = time.time() - start_time
            logger.info(f"Write operation completed in {write_time:.2f} seconds")

            # Update statistics from write operation
            self.stats["updated_records"] = write_result.get("updated_records", 0)
            self.stats["new_records"] = write_result.get("new_records", 0)

            logger.info(f"Pipeline completed successfully. Processed {self.stats['successful_records']} records.")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            self.stats["error"] = str(e)

        self.stats["end_time"] = datetime.now().isoformat()
        return self.stats

    def process_format_type(self, format_type: str, from_date: str, to_date: str) -> Dict[str, Any]:
        """
        Process data for a specific format type and date range.

        Args:
            format_type: Format type ("arXiv" or "arXivRaw")
            from_date: Start date in ISO format (YYYY-MM-DD), inclusive
            to_date: End date in ISO format (YYYY-MM-DD), exclusive

        Returns:
            Dictionary with processing statistics
        """
        self.stats["start_time"] = datetime.now().isoformat()

        try:
            logger.info(f"Starting ArXiv data processing pipeline for {format_type} from {from_date} to {to_date}")

            # Step 1: Extract raw data
            logger.info(f"Extracting {format_type} data from source")
            start_time = time.time()
            raw_data = self.extractor.extract_format_type(format_type, from_date, to_date)
            extract_time = time.time() - start_time
            logger.info(f"Extraction completed in {extract_time:.2f} seconds")

            self.stats["total_records"] = len(raw_data)
            if not raw_data:
                logger.info("No data found for the specified parameters")
                self.stats["end_time"] = datetime.now().isoformat()
                return self.stats

            # Step 2: Transform data
            logger.info("Transforming metadata")
            start_time = time.time()
            transformed_data = self.transformer.transform(raw_data, format_type=format_type)
            transform_time = time.time() - start_time
            logger.info(f"Transformation completed in {transform_time:.2f} seconds")

            self.stats["successful_records"] = len(transformed_data)
            self.stats["failed_records"] = self.stats["total_records"] - self.stats["successful_records"]

            # Step 3: Write to persistence layer
            logger.info("Writing data to persistence layer")
            start_time = time.time()
            write_result = self.writer.write(transformed_data)
            write_time = time.time() - start_time
            logger.info(f"Write operation completed in {write_time:.2f} seconds")

            # Update statistics from write operation
            self.stats["updated_records"] = write_result.get("updated_records", 0)
            self.stats["new_records"] = write_result.get("new_records", 0)

            logger.info(f"Pipeline completed successfully. Processed {self.stats['successful_records']} records.")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            self.stats["error"] = str(e)

        self.stats["end_time"] = datetime.now().isoformat()
        return self.stats
