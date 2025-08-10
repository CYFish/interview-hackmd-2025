import sys
import logging
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pipeline import Pipeline


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_args():
    """
    Process job arguments.

    Returns:
        Dictionary with processed arguments
    """
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "database_name",
        "table_name",
        "input_path",
        "output_path",
        "bucket",
        "generate_models",
        "write_models",
        "use_cloudwatch",
        "region",
    ])

    # Convert string boolean parameters to actual booleans
    for bool_param in ['generate_models', 'write_models', 'use_cloudwatch']:
        if bool_param in args:
            args[bool_param] = args[bool_param].lower() == 'true'

    return args


def main():
    """
    Main entry point for the Glue job.
    """
    # Initialize Glue context
    args = process_args()
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    # Configure the pipeline
    config = {
        "database_name": args["database_name"],
        "table_name": args["table_name"],
        "input_path": args["input_path"],
        "output_path": args["output_path"],
        "bucket": args["bucket"],
        "generate_models": args.get("generate_models", False),
        "write_models": args.get("write_models", False),
        "use_cloudwatch": args.get("use_cloudwatch", False),
        "update_mode": "overwrite",  # Always overwrite for historical data
        "region": args.get("region", "ap-northeast-1"),
        "job_name": args["JOB_NAME"],
        "run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
    }

    # Create pipeline and run
    pipeline = Pipeline(glue_context, config)
    result = pipeline.run()

    # Display results
    print(f"Processing result: {result}")

    # Commit the job
    job.commit()


if __name__ == "__main__":
    main()
