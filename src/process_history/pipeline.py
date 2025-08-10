import logging
from typing import Dict, Any
from datetime import datetime

from extractor import Extractor
from transformer import Transformer
from quality import QualityChecker
from writer import Writer
from model import ModelGenerator
from metric import MetricsCollector


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Pipeline:
    """
    Main pipeline orchestrator for ArXiv data processing using AWS Glue.

    This class coordinates the entire data processing workflow:
    1. Data extraction from source (S3)
    2. Data transformation and cleaning
    3. Data quality checks
    4. Writing processed data to destination
    5. Generating relational data models
    6. Metrics collection and reporting
    """

    def __init__(self, glue_context, config: Dict[str, Any]):
        """
        Initialize the pipeline with configuration.

        Args:
            glue_context: The GlueContext for the current job
            config: Configuration dictionary containing processing parameters
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.config = config

        # Initialize components
        self.extractor = Extractor(glue_context, config)
        self.transformer = Transformer(glue_context, config)
        self.quality_checker = QualityChecker(glue_context, config)
        self.writer = Writer(glue_context, config)
        self.model_generator = ModelGenerator(glue_context, config)
        self.metrics = MetricsCollector(glue_context, config)

        # Initialize statistics
        self.stats = {
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "skipped_records": 0,
            "start_time": None,
            "end_time": None,
        }

    def run(self) -> Dict[str, Any]:
        """
        Execute the complete data processing pipeline.

        Returns:
            Dictionary with processing statistics and results
        """
        try:
            self.stats["start_time"] = datetime.now()
            logger.info("Starting ArXiv data processing pipeline")
            self.metrics.start_timer("pipeline_total")

            # Step 1: Extract raw data
            logger.info("Extracting data")
            self.metrics.start_timer("extract")
            raw_df = self.extractor.extract()
            self.metrics.end_timer("extract")
            self.stats["total_records"] = raw_df.count()

            if raw_df.count() == 0:
                logger.info("No records to process")
                self.metrics.end_timer("pipeline_total")
                self.metrics.publish()
                self.stats["end_time"] = datetime.now()

                return {
                    "status": "success",
                    "total_records": 0,
                    "successful_records": 0,
                    "failed_records": 0,
                    "skipped_records": 0,
                    "records_written": 0,
                    "data_quality": {},
                    "metrics": self.metrics.get_metrics(),
                }

            # Step 2: Transform and clean data
            logger.info("Transforming data")
            self.metrics.start_timer("transform")
            transformed_df = self.transformer.transform(raw_df)
            self.metrics.end_timer("transform")
            self.stats["successful_records"] = transformed_df.count()
            self.stats["failed_records"] = self.stats["total_records"] - self.stats["successful_records"]

            # Step 3: Check data quality
            logger.info("Checking data quality")
            self.metrics.start_timer("quality_check")
            quality_report = self.quality_checker.check(transformed_df)
            self.metrics.end_timer("quality_check")

            # Step 4: Generate data models
            logger.info("Generating data models")
            self.metrics.start_timer("generate_models")
            if self.config.get("generate_models", False):
                data_models = self.model_generator.generate_models(transformed_df)
                # Write models if configured
                if self.config.get("write_models", False):
                    self.model_generator.write_models(data_models)
            self.metrics.end_timer("generate_models")

            # Step 5: Write processed data
            logger.info("Writing processed data")
            self.metrics.start_timer("write")
            write_result = self.writer.write(transformed_df)
            self.metrics.end_timer("write")

            # Step 6: Collect resource metrics
            self.metrics.collect_resource_metrics()

            # Step 7: Publish metrics
            self.metrics.end_timer("pipeline_total")
            self.metrics.publish()

            # Prepare result
            self.stats["end_time"] = datetime.now()
            result = {
                "status": "success",
                "total_records": self.stats["total_records"],
                "successful_records": self.stats["successful_records"],
                "failed_records": self.stats["failed_records"],
                "skipped_records": self.stats.get("skipped_records", 0),
                "records_written": write_result.get("records_written", 0),
                "data_quality": quality_report,
                "metrics": self.metrics.get_metrics(),
            }

            logger.info(f"Pipeline completed successfully. Processed {self.stats['successful_records']} records.")
            return result

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self.metrics.record_metric("pipeline_error", 1)
            self.metrics.publish()
            self.stats["end_time"] = datetime.now()

            return {
                "status": "error",
                "error": str(e),
                "total_records": self.stats["total_records"],
                "successful_records": self.stats["successful_records"],
                "failed_records": self.stats["failed_records"],
                "skipped_records": self.stats.get("skipped_records", 0),
            }

    def get_stats(self) -> Dict[str, Any]:
        """
        Get pipeline statistics.

        Returns:
            Dictionary with pipeline statistics
        """
        if self.stats["start_time"] and self.stats["end_time"]:
            duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            self.stats["duration_seconds"] = duration

        return self.stats
