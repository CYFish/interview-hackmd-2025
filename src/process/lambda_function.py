import json
import logging

from processor import ArxivProcessor


def lambda_handler(event, context):
    """AWS Lambda handler function"""
    logging.info(f"Received event for {event}")
    input_prefix = event["input_prefix"]
    output_prefix = event.get("output_prefix", "processed/")
    chunk_size = event.get("chunk_size", 20000)

    try:
        processor = ArxivProcessor(
            input_prefix=input_prefix,
            output_prefix=output_prefix,
            chunk_size=chunk_size,
        )
        processor.process()

        return {
            "statusCode": 200,
            "body": json.dumps("Data processed successfully")
        }
    except Exception as e:
        logging.error(e)
        raise


if __name__ == "__main__":
    # For local testing
    result = lambda_handler({}, {})
    print(json.dumps(result, indent=2))
