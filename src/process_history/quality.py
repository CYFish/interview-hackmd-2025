import logging
from typing import Any, Dict, List
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, sum as spark_sum, length, size


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class QualityChecker:
    """
    Responsible for performing data quality checks on processed data.

    This class analyzes data for completeness, consistency, and anomalies,
    and generates quality reports.
    """

    def __init__(self, glue_context, config: Dict[str, Any]):
        """
        Initialize the Glue data quality checker.

        Args:
            glue_context: The GlueContext for the current job
            config: Configuration dictionary with quality check parameters
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.config = config

        # Quality thresholds
        self.thresholds = config.get("quality_thresholds", {
            "missing_titles_pct": 5.0,
            "missing_abstracts_pct": 10.0,
            "missing_categories_pct": 5.0,
            "missing_authors_pct": 15.0,
        })

        # Statistics
        self.stats = {
            "total_records": 0,
            "start_time": None,
            "end_time": None,
        }

    def check(self, df: DataFrame) -> Dict[str, Any]:
        """
        Perform data quality checks on the DataFrame.

        Args:
            df: DataFrame to check

        Returns:
            Dictionary with quality metrics
        """
        self.stats["start_time"] = datetime.now()
        self.stats["total_records"] = df.count()
        logger.info(f"Checking data quality for {self.stats['total_records']} records")

        if self.stats["total_records"] == 0:
            logger.warning("No records to check for quality")
            self.stats["end_time"] = datetime.now()
            return {
                "status": "warning",
                "message": "No records to check",
                "metrics": {},
                "anomalies": [],
                "passed": True
            }

        try:
            # Calculate completeness metrics
            completeness = self._check_completeness(df)

            # Check for anomalies
            anomalies = self._check_anomalies(df)

            # Check consistency
            consistency = self._check_consistency(df)

            # Determine if quality checks passed
            passed = True
            failures = []

            for metric, value in completeness.items():
                threshold_key = f"{metric}_pct"
                if threshold_key in self.thresholds and value > self.thresholds[threshold_key]:
                    passed = False
                    failures.append(f"{metric}: {value}% exceeds threshold of {self.thresholds[threshold_key]}%")

            # Combine all metrics
            metrics = {
                **completeness,
                **consistency
            }

            # Create quality report
            quality_report = {
                "status": "passed" if passed else "failed",
                "message": "All quality checks passed" if passed else f"Quality checks failed: {', '.join(failures)}",
                "metrics": metrics,
                "anomalies": anomalies,
                "passed": passed
            }

            logger.info(f"Data quality check {'passed' if passed else 'failed'}")
            self.stats["end_time"] = datetime.now()
            return quality_report

        except Exception as e:
            logger.error(f"Error checking data quality: {e}")
            self.stats["end_time"] = datetime.now()
            return {
                "status": "error",
                "message": f"Error checking data quality: {str(e)}",
                "metrics": {},
                "anomalies": [],
                "passed": False
            }

    def _check_completeness(self, df: DataFrame) -> Dict[str, float]:
        """
        Check data completeness (missing values).

        Args:
            df: DataFrame to check

        Returns:
            Dictionary with completeness metrics
        """
        # Calculate missing value percentages
        total_count = float(df.count())

        if total_count == 0:
            return {
                "missing_titles_pct": 0.0,
                "missing_abstracts_pct": 0.0,
                "missing_categories_pct": 0.0,
                "missing_authors_pct": 0.0,
            }

        # Count missing values for each field
        missing_counts = df.select(
            (spark_sum(when(col("title").isNull() | (col("title") == ""), 1).otherwise(0)) / total_count * 100).alias("missing_titles_pct"),
            (spark_sum(when(col("abstract").isNull() | (col("abstract") == ""), 1).otherwise(0)) / total_count * 100).alias("missing_abstracts_pct"),
            (spark_sum(when(col("categories").isNull() | (col("categories") == ""), 1).otherwise(0)) / total_count * 100).alias("missing_categories_pct"),
            (spark_sum(when(col("authors_parsed").isNull() | (col("authors_parsed") == "[]"), 1).otherwise(0)) / total_count * 100).alias("missing_authors_pct"),
        ).collect()[0]

        # Convert to dictionary
        completeness = {
            "missing_titles_pct": missing_counts["missing_titles_pct"],
            "missing_abstracts_pct": missing_counts["missing_abstracts_pct"],
            "missing_categories_pct": missing_counts["missing_categories_pct"],
            "missing_authors_pct": missing_counts["missing_authors_pct"],
        }

        return completeness

    def _check_anomalies(self, df: DataFrame) -> List[Dict[str, Any]]:
        """
        Check for data anomalies.

        Args:
            df: DataFrame to check

        Returns:
            List of anomaly dictionaries
        """
        anomalies = []

        # Check for future dates
        now = datetime.now().date()
        future_dates = df.filter(col("submitted_date") > now).count()

        if future_dates > 0:
            anomalies.append({
                "type": "future_date",
                "field": "submitted_date",
                "count": future_dates,
                "description": f"Found {future_dates} records with submission dates in the future"
            })

        # Check for extremely old dates (before 1990)
        old_dates = df.filter(col("submitted_date") < "1990-01-01").count()

        if old_dates > 0:
            anomalies.append({
                "type": "old_date",
                "field": "submitted_date",
                "count": old_dates,
                "description": f"Found {old_dates} records with submission dates before 1990"
            })

        # Check for extremely long titles
        long_titles = df.filter(length(col("title")) > 500).count()

        if long_titles > 0:
            anomalies.append({
                "type": "long_title",
                "field": "title",
                "count": long_titles,
                "description": f"Found {long_titles} records with unusually long titles (>500 chars)"
            })

        return anomalies

    def _check_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """
        Check data consistency.

        Args:
            df: DataFrame to check

        Returns:
            Dictionary with consistency metrics
        """
        consistency = {}

        # Check if update_date is after submitted_date
        if "update_date" in df.columns and "submitted_date" in df.columns:
            inconsistent_dates = df.filter(col("update_date") < col("submitted_date")).count()
            consistency["inconsistent_dates_pct"] = (inconsistent_dates / float(df.count())) * 100 if df.count() > 0 else 0.0

        # Check if version_count matches versions array length
        if "version_count" in df.columns and "versions" in df.columns:
            inconsistent_versions = df.filter(col("version_count") != size(col("versions"))).count()
            consistency["inconsistent_versions_pct"] = (inconsistent_versions / float(df.count())) * 100 if df.count() > 0 else 0.0

        return consistency

    def get_stats(self) -> Dict[str, Any]:
        """
        Get quality check statistics.

        Returns:
            Dictionary with quality check statistics
        """
        if self.stats["start_time"] and self.stats["end_time"]:
            duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            self.stats["duration_seconds"] = duration

        return self.stats
