import json
import logging

import pendulum

from collector import Collector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    """
    AWS Lambda function handler for ArXiv data collection.

    Args:
        event (dict): Lambda event data, may contain:
            - from_date: Start date in ISO format (YYYY-MM-DD), inclusive
            - to_date: End date in ISO format (YYYY-MM-DD), exclusive
            - bucket: S3 bucket name for storing data
            - batch_size: Number of records per batch
        context: Lambda context

    Returns:
        dict: Collection statistics and status
    """
    try:
        logger.info(f"Received event: {json.dumps(event)}")

        # Extract parameters from event or use defaults
        from_date = event.get("from_date")
        to_date = event.get("to_date")
        bucket = event.get("bucket", "hackmd-project-2025")
        batch_size = int(event.get("batch_size", 1000))

        # If dates are not provided, default to collecting yesterday's data
        if not from_date or not to_date:
            timezone = "UTC"
            from_date = pendulum.now(timezone).subtract(days=1).to_date_string()
            to_date = pendulum.now(timezone).to_date_string()

        logger.info(f"Collecting data from {from_date} to {to_date}")

        # Create collector with S3 storage
        collector = Collector(
            from_str=from_date,
            to_str=to_date,
            use_s3=True,
            bucket=bucket,
            batch_size=batch_size
        )

        # Run collection
        stats = collector.collect()

        # Return results
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Collection completed successfully",
                "stats": stats
            }),
            "stats": stats
        }

    except Exception as e:
        logger.error(f"Error in lambda function: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": f"Error: {str(e)}"
            })
        }
