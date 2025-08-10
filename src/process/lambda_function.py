import json
import logging

import pendulum

from pipeline import Pipeline


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    """
    AWS Lambda function handler for ArXiv data processing.

    Args:
        event (dict): Lambda event data, may contain:
            - from_date: Start date in ISO format (YYYY-MM-DD), inclusive
            - to_date: End date in ISO format (YYYY-MM-DD), exclusive
            - s3_bucket: S3 bucket name for source data
            - table_name: DynamoDB table name for destination
        context: Lambda context

    Returns:
        dict: Processing statistics and status
    """
    try:
        logger.info(f"Received event: {json.dumps(event)}")

        # Extract parameters from event or use defaults
        from_date = event.get("from_date")
        to_date = event.get("to_date")
        s3_bucket = event.get("s3_bucket", "hackmd-project-2025")
        table_name = event.get("table_name", "arxiv-papers")
        region_name = event.get("region_name", "ap-northeast-1")

        # If dates are not provided, default to processing yesterday's data
        if not from_date or not to_date:
            timezone = "UTC"
            from_date = pendulum.now(timezone).subtract(days=1).to_date_string()
            to_date = pendulum.now(timezone).to_date_string()

        logger.info(f"Processing data from {from_date} to {to_date}")

        # Create pipeline configuration
        config = {
            "region_name": region_name,
            "s3_bucket": s3_bucket,
            "table_name": table_name,
            "batch_size": 25
        }

        # Create and run pipeline
        pipeline = Pipeline(config)

        # Process data
        logger.info("Processing raw data")
        stats = pipeline.process_date_range(from_date, to_date)

        # Return results
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processing completed successfully",
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


if __name__ == "__main__":
    # For local testing
    import argparse

    parser = argparse.ArgumentParser(description="Process ArXiv data")
    parser.add_argument("--from-date", type=str, required=True,
                        help="Start date (YYYY-MM-DD), inclusive")
    parser.add_argument("--to-date", type=str, required=True,
                        help="End date (YYYY-MM-DD), exclusive")
    parser.add_argument("--format-type", type=str, choices=["arXiv", "arXivRaw"],
                        help="Format type to process (optional)")
    parser.add_argument("--s3-bucket", type=str, default="hackmd-project-2025",
                        help="S3 bucket name")
    parser.add_argument("--table-name", type=str, default="arxiv-papers",
                        help="DynamoDB table name")
    parser.add_argument("--region-name", type=str, default="ap-northeast-1",
                        help="AWS region name")
    parser.add_argument("--local-mode", action="store_true",
                        help="Use local file system instead of S3")
    parser.add_argument("--local-dir", type=str, default="data",
                        help="Local directory for data (when using local mode)")

    args = parser.parse_args()

    # Create event for lambda_handler
    event = {
        "from_date": args.from_date,
        "to_date": args.to_date,
        "format_type": args.format_type,
        "s3_bucket": args.s3_bucket,
        "table_name": args.table_name,
        "region_name": args.region_name,
        "local_mode": args.local_mode,
        "local_dir": args.local_dir
    }

    # Call lambda_handler
    result = lambda_handler(event, None)

    # Print result
    print(json.dumps(result, indent=2))
