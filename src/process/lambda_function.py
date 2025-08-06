import json
import logging

from processor import ArxivProcessor


def lambda_handler(event, context):
    """AWS Lambda handler function"""
    try:
        processor = ArxivProcessor(
            input_prefix="raw/initial/arxiv-metadata-oai-snapshot.json",
            output_prefix="processed/",
            chunk_size=2,
        )
        processor.process()

        return {
            "statusCode": 200,
            "body": "Processed successfully"
        }
    except Exception as e:
        logging.error(e)
        raise


if __name__ == "__main__":
    # For local testing
    result = lambda_handler({}, {})
    print(json.dumps(result, indent=2))
