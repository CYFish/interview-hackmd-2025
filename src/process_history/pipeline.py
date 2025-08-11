import logging
import time
from datetime import datetime
from typing import Any, Dict

from extractor import Extractor
from transformer import Transformer
from writer import Writer


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Pipeline:
    """
    Main pipeline orchestrator for ArXiv historical data processing.

    This class coordinates the entire data processing workflow:
    1. Data extraction from historical file
    2. Metadata transformation and enrichment
    3. Writing processed data to Parquet files
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the pipeline with configuration.

        Args:
            config: Configuration dictionary containing processing parameters
        """
        self.config = config
        self.extractor = Extractor(config)
        self.transformer = Transformer(config)
        self.writer = Writer(config)

        # Initialize statistics
        self.stats = {
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "records_written": 0,
            "start_time": None,
            "end_time": None,
            "data_quality": {}
        }

    def process(self) -> Dict[str, Any]:
        """
        Process historical ArXiv data.

        Returns:
            Dictionary with processing statistics
        """
        self.stats["start_time"] = datetime.now().isoformat()

        try:
            logger.info("Starting ArXiv historical data processing pipeline")

            # Step 1: Extract raw data
            logger.info("Extracting data from source")
            start_time = time.time()
            raw_data = self.extractor.extract()
            extract_time = time.time() - start_time
            logger.info(f"Extraction completed in {extract_time:.2f} seconds")

            # Get extraction statistics
            extract_stats = self.extractor.get_stats()
            self.stats["total_records"] = extract_stats["total_records"]

            if not raw_data:
                logger.info("No data found in the historical file")
                self.stats["end_time"] = datetime.now().isoformat()
                return self.stats

            # Step 2: Transform data
            logger.info("Transforming metadata")
            start_time = time.time()
            transformed_data = self.transformer.transform(raw_data)
            transform_time = time.time() - start_time
            logger.info(f"Transformation completed in {transform_time:.2f} seconds")

            # Get transformation statistics
            transform_stats = self.transformer.get_stats()
            self.stats["successful_records"] = transform_stats["output_records"]
            self.stats["failed_records"] = transform_stats["failed_records"]
            self.stats["data_quality"] = transform_stats["data_quality"]

            # Step 3: Write to Parquet files
            logger.info("Writing data to Parquet files")
            start_time = time.time()
            write_result = self.writer.write(transformed_data)
            write_time = time.time() - start_time
            logger.info(f"Write operation completed in {write_time:.2f} seconds")

            # Get write statistics
            write_stats = self.writer.get_stats()
            self.stats["records_written"] = write_stats["successful_records"]

            logger.info(f"Pipeline completed successfully. Processed {self.stats['successful_records']} records, "
                        f"wrote {self.stats['records_written']} records.")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            self.stats["error"] = str(e)

        self.stats["end_time"] = datetime.now().isoformat()
        return self.stats
