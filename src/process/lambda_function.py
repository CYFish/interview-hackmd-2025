import json
import logging

from processor import ArxivProcessor


def lambda_handler(event, context):
    """AWS Lambda handler function"""
    logging.info(f"Received event for {event}")
    input_prefix = event.get("input_prefix", None)
    output_prefix = event.get("output_prefix", None)

    try:
        processor = ArxivProcessor(
            input_prefix="raw/initial/arxiv-metadata-oai-snapshot.json" if input_prefix is None else input_prefix,
            output_prefix="processed/" if output_prefix is None else output_prefix,
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
