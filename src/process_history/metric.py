import logging
import time
from typing import Dict, Any
from datetime import datetime

import boto3


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MetricsCollector:
    """
    Responsible for collecting and publishing metrics for monitoring.

    This class tracks processing times, record counts, and resource utilization,
    and can publish metrics to CloudWatch.
    """

    def __init__(self, glue_context, config: Dict[str, Any]):
        """
        Initialize the Glue metrics collector.

        Args:
            glue_context: The GlueContext for the current job
            config: Configuration dictionary with metrics parameters
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.config = config

        # Extract configuration
        self.use_cloudwatch = config.get("use_cloudwatch", False)
        self.namespace = config.get("cloudwatch_namespace", "ArxivProcessor")
        self.region = config.get("region", "ap-northeast-1")
        self.job_name = config.get("job_name", "arxiv-processor")
        self.run_id = config.get("run_id", datetime.now().strftime("%Y%m%d_%H%M%S"))

        # Initialize metrics storage
        self.metrics = {}
        self.timers = {}

        # Initialize CloudWatch client if needed
        self.cloudwatch = None
        if self.use_cloudwatch:
            try:
                self.cloudwatch = boto3.client('cloudwatch', region_name=self.region)
                logger.info("CloudWatch metrics enabled")
            except Exception as e:
                logger.error(f"Error initializing CloudWatch client: {e}")
                self.use_cloudwatch = False

    def start_timer(self, name: str) -> None:
        """
        Start a timer for measuring duration.

        Args:
            name: Name of the timer
        """
        self.timers[name] = {
            "start": time.time(),
            "end": None,
            "duration": None
        }

    def end_timer(self, name: str) -> float:
        """
        End a timer and record the duration.

        Args:
            name: Name of the timer

        Returns:
            Duration in seconds
        """
        if name in self.timers:
            self.timers[name]["end"] = time.time()
            duration = self.timers[name]["end"] - self.timers[name]["start"]
            self.timers[name]["duration"] = duration

            # Record as a metric
            self.record_metric(f"{name}_duration", duration)

            return duration
        else:
            logger.warning(f"Timer '{name}' was not started")
            return 0.0

    def record_metric(self, name: str, value: Any) -> None:
        """
        Record a metric value.

        Args:
            name: Name of the metric
            value: Value of the metric
        """
        # Store locally
        self.metrics[name] = value

        # Log the metric
        logger.info(f"Metric: {name} = {value}")

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get all recorded metrics.

        Returns:
            Dictionary with all metrics
        """
        # Add timer durations to metrics
        for name, timer in self.timers.items():
            if timer["duration"] is not None:
                self.metrics[f"{name}_duration"] = timer["duration"]

        return self.metrics

    def publish(self) -> Dict[str, Any]:
        """
        Publish metrics to CloudWatch.

        Returns:
            Dictionary with publish operation results
        """
        if not self.use_cloudwatch or not self.cloudwatch:
            logger.info("CloudWatch metrics disabled, not publishing")
            return {"status": "skipped", "reason": "CloudWatch disabled"}

        try:
            # Prepare metric data
            metric_data = []

            for name, value in self.metrics.items():
                # Skip non-numeric values
                if not isinstance(value, (int, float)):
                    continue

                metric_data.append({
                    'MetricName': name,
                    'Value': float(value),
                    'Unit': 'Seconds' if name.endswith('_duration') else 'Count',
                    'Dimensions': [
                        {
                            'Name': 'JobName',
                            'Value': self.job_name
                        },
                        {
                            'Name': 'RunId',
                            'Value': self.run_id
                        }
                    ]
                })

            # Split into batches of 20 (CloudWatch limit)
            batch_size = 20
            for i in range(0, len(metric_data), batch_size):
                batch = metric_data[i:i+batch_size]

                self.cloudwatch.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch
                )

            logger.info(f"Published {len(metric_data)} metrics to CloudWatch")
            return {"status": "success", "metrics_published": len(metric_data)}

        except Exception as e:
            logger.error(f"Error publishing metrics to CloudWatch: {e}")
            return {"status": "error", "error": str(e)}

    def collect_resource_metrics(self) -> Dict[str, float]:
        """
        Collect resource utilization metrics from the Spark context.

        Returns:
            Dictionary with resource metrics
        """
        try:
            # Get Spark metrics
            spark_metrics = {}

            # Get executor metrics if available
            spark_context = self.spark.sparkContext
            if hasattr(spark_context, "getExecutorMemoryStatus"):
                executor_memory = spark_context.getExecutorMemoryStatus()
                if executor_memory:
                    total_memory = sum(mem[0] for mem in executor_memory.values())
                    used_memory = sum(mem[1] for mem in executor_memory.values())

                    spark_metrics["executor_memory_total_mb"] = total_memory / (1024 * 1024)
                    spark_metrics["executor_memory_used_mb"] = used_memory / (1024 * 1024)
                    spark_metrics["executor_memory_used_pct"] = (used_memory / total_memory) * 100 if total_memory > 0 else 0

            # Record all metrics
            for name, value in spark_metrics.items():
                self.record_metric(name, value)

            return spark_metrics

        except Exception as e:
            logger.error(f"Error collecting resource metrics: {e}")
            return {}
