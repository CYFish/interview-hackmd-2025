import argparse
import logging
import sys
from typing import Dict, Any

from pipeline import Pipeline


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args() -> Dict[str, Any]:
    """
    Parse command line arguments.

    Returns:
        Dictionary with parsed arguments
    """
    parser = argparse.ArgumentParser(description="Process historical ArXiv metadata")
    parser.add_argument("--input-path", type=str, required=True,
                        help="Path to JSON file containing ArXiv data (local path or S3 key)")
    parser.add_argument("--output-path", type=str, required=True,
                        help="Path for output Parquet files (S3 prefix or local directory)")
    parser.add_argument("--input-local", action="store_true",
                        help="Read input from local file system instead of S3")
    parser.add_argument("--output-local", action="store_true",
                        help="Write output to local file system instead of S3")
    parser.add_argument("--bucket", type=str, default="hackmd-project-2025",
                        help="S3 bucket name (only used if not in local mode)")
    parser.add_argument("--chunk-size", type=int, default=10000,
                        help="Number of records to process in each batch")

    args = parser.parse_args()

    # Convert to config dictionary
    config = {
        "input_path": args.input_path,
        "output_path": args.output_path,
        "input_local": args.input_local,
        "output_local": args.output_local,
        "s3_bucket": args.bucket,
        "batch_size": args.chunk_size,
    }

    return config


def main() -> int:
    """
    Main function for command line execution.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        # Parse command line arguments
        config = parse_args()

        # Create pipeline
        pipeline = Pipeline(config)

        # Run processing
        result = pipeline.process()

        # Display results
        # Get input and output modes
        input_mode = "local" if config["input_local"] else "S3"
        output_mode = "local" if config["output_local"] else "S3"

        print("\nProcessing completed!")
        print(f"Input source: {input_mode}")
        print(f"Output destination: {output_mode}")
        print(f"Total records: {result['total_records']}")
        print(f"Successful records: {result['successful_records']}")
        print(f"Failed records: {result['failed_records']}")
        print(f"Records written: {result['records_written']}")
        print(f"Start time: {result['start_time']}")
        print(f"End time: {result['end_time']}")

        # Print data quality metrics
        if "data_quality" in result:
            print("\nData Quality Report:")
            quality = result["data_quality"]
            if "missing_titles_pct" in quality:
                print(f"Missing titles: {quality['missing_titles_pct']}%")
            if "missing_abstracts_pct" in quality:
                print(f"Missing abstracts: {quality['missing_abstracts_pct']}%")
            if "missing_categories_pct" in quality:
                print(f"Missing categories: {quality['missing_categories_pct']}%")
            if "missing_authors_pct" in quality:
                print(f"Missing authors: {quality['missing_authors_pct']}%")
            if "anomalies" in quality and quality["anomalies"]:
                print(f"Anomalies detected: {len(quality['anomalies'])}")

        return 0  # Success

    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)
        return 1  # Failure


if __name__ == "__main__":
    sys.exit(main())
