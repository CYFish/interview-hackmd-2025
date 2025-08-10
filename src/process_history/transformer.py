import logging
from typing import Dict, Any
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, to_date, size, array, when, regexp_replace,
    datediff, date_format, expr, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, ArrayType, DateType
)


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Transformer:
    """
    Responsible for transforming raw ArXiv historical data using AWS Glue.

    This class handles data cleaning, normalization, and feature extraction
    for the ArXiv metadata dataset, including derived metrics calculation.
    """

    def __init__(self, glue_context, config: Dict[str, Any]):
        """
        Initialize the Glue data transformer for ArXiv historical data.

        Args:
            glue_context: The GlueContext for the current job
            config: Configuration dictionary with transformation parameters
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.config = config
        self.time_zone = config.get("time_zone", "UTC")

        # Statistics tracking
        self.stats = {
            "input_records": 0,
            "output_records": 0,
            "failed_records": 0,
            "start_time": None,
            "end_time": None,
            "duration_seconds": 0
        }

        # Define schema for the output data
        self.output_schema = StructType([
            # Core paper metadata
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("abstract", StringType(), True),
            StructField("categories", StringType(), True),
            StructField("doi", StringType(), True),

            # Temporal data
            StructField("submitted_date", DateType(), True),
            StructField("update_date", DateType(), True),
            StructField("published_date", DateType(), True),
            StructField("version_count", IntegerType(), True),

            # Author and institution data
            StructField("authors_parsed", ArrayType(ArrayType(StringType())), True),
            StructField("institutions", StringType(), True),

            # Publication data
            StructField("journal_ref", StringType(), True),
            StructField("is_published", BooleanType(), True),

            # Additional fields
            StructField("update_frequency", FloatType(), True),
            StructField("submission_to_publication", FloatType(), True),

            # Partition columns
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            StructField("day", StringType(), True),
        ])

    def transform(self, raw_df: DataFrame) -> DataFrame:
        """
        Transform raw ArXiv historical data into a structured format.

        Args:
            raw_df: Raw DataFrame with ArXiv metadata

        Returns:
            Transformed DataFrame with normalized fields and derived metrics
        """
        self.stats["start_time"] = datetime.now()
        input_count = raw_df.count()
        self.stats["input_records"] = input_count
        logger.info(f"Transforming {input_count} ArXiv records")

        try:
            # Return empty DataFrame if no input data
            if input_count == 0:
                logger.warning("No records to transform")
                transformed_df = self.spark.createDataFrame([], self.output_schema)
                self.stats["end_time"] = datetime.now()
                return transformed_df

            # Process the data
            df = self._clean_core_metadata(raw_df)
            df = self._process_dates(df)
            df = self._process_versions(df)
            df = self._process_authors(df)
            df = self._process_publication_info(df)
            df = self._calculate_derived_metrics(df)
            df = self._create_partition_columns(df)

            # Select only the columns in our schema
            column_list = [field.name for field in self.output_schema.fields]
            transformed_df = df.select(*column_list)

            # Count successful transformations
            output_count = transformed_df.count()
            self.stats["output_records"] = output_count
            self.stats["failed_records"] = input_count - output_count

            logger.info(f"Successfully transformed {output_count} records ({self.stats['failed_records']} failed)")
            self.stats["end_time"] = datetime.now()
            return transformed_df

        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}", exc_info=True)
            self.stats["end_time"] = datetime.now()
            raise RuntimeError(f"Failed to transform ArXiv data: {str(e)}") from e

    def _clean_core_metadata(self, df: DataFrame) -> DataFrame:
        """
        Clean and normalize core metadata fields.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with cleaned core metadata
        """
        logger.info("Cleaning core metadata fields")

        # Clean and normalize text fields
        if "id" in df.columns:
            df = df.withColumn("id", col("id").cast(StringType()))

        if "title" in df.columns:
            # Clean title: remove extra whitespace and normalize
            df = df.withColumn(
                "title",
                regexp_replace(regexp_replace(col("title"), "\n", " "), "\\s+", " ")
            )
        else:
            df = df.withColumn("title", lit(None).cast(StringType()))

        if "abstract" in df.columns:
            # Clean abstract: remove extra whitespace and normalize
            df = df.withColumn(
                "abstract",
                regexp_replace(regexp_replace(col("abstract"), "\n", " "), "\\s+", " ")
            )
        else:
            df = df.withColumn("abstract", lit(None).cast(StringType()))

        if "categories" in df.columns:
            df = df.withColumn("categories", col("categories").cast(StringType()))
        else:
            df = df.withColumn("categories", lit(None).cast(StringType()))

        if "doi" in df.columns:
            df = df.withColumn("doi", col("doi").cast(StringType()))
        else:
            df = df.withColumn("doi", lit(None).cast(StringType()))

        return df

    def _process_dates(self, df: DataFrame) -> DataFrame:
        """
        Process and normalize date fields.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with processed date fields
        """
        logger.info("Processing date fields")

        # Parse update_date if available
        if "update_date" in df.columns:
            df = df.withColumn("update_date", to_date(col("update_date")))
        else:
            df = df.withColumn("update_date", lit(None).cast(DateType()))

        # Initialize submitted_date and published_date (will be updated in version processing)
        df = df.withColumn("submitted_date", lit(None).cast(DateType()))
        df = df.withColumn("published_date", lit(None).cast(DateType()))

        return df

    def _process_versions(self, df: DataFrame) -> DataFrame:
        """
        Process version information and extract date information.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with processed version information
        """
        logger.info("Processing version information")

        # Process versions array if it exists
        if "versions" in df.columns:
            # Count versions
            df = df.withColumn("version_count", size(col("versions")))

            # Use to_timestamp with format string to parse version dates
            # Format for "Mon, 2 Apr 2007 19:18:42 GMT"
            date_format_str = "EEE, d MMM yyyy HH:mm:ss z"

            # Extract first version date (submission date)
            df = df.withColumn(
                "submitted_date",
                when(
                    size(col("versions")) > 0,
                    to_date(to_timestamp(expr("versions[0].created"), date_format_str))
                ).otherwise(lit(None).cast(DateType()))
            )

            # If journal ref exists, use the last version date as publication date
            df = df.withColumn(
                "published_date",
                when(
                    col("journal-ref").isNotNull() & (size(col("versions")) > 0),
                    to_date(to_timestamp(
                        expr(f"versions[{size(col('versions'))-1}].created"),
                        date_format_str
                    ))
                ).otherwise(lit(None).cast(DateType()))
            )
        else:
            # Default values if no versions
            df = df.withColumn("version_count", lit(1).cast(IntegerType()))

        return df

    def _process_authors(self, df: DataFrame) -> DataFrame:
        """
        Process author information and extract institutions.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with processed author information
        """
        logger.info("Processing author information")

        # Handle authors_parsed array
        if "authors_parsed" in df.columns:
            # Ensure it's the right format (array of arrays)
            if df.select("authors_parsed").dtypes[0][1].startswith("array<array<string>>"):
                # Already in the right format
                pass
            elif df.select("authors_parsed").dtypes[0][1].startswith("array<string>"):
                # Convert array of strings to array of array of strings
                df = df.withColumn("authors_parsed", array(col("authors_parsed")))
            else:
                # Default empty array
                df = df.withColumn("authors_parsed", lit(array()).cast("array<array<string>>"))
        else:
            df = df.withColumn("authors_parsed", lit(array()).cast("array<array<string>>"))

        # Extract institution from submitter if available
        if "submitter" in df.columns:
            # Try to extract email domain as institution
            df = df.withColumn(
                "institutions",
                when(
                    col("submitter").contains("@"),
                    expr("split(split(submitter, '@')[1], '\\\\.')[0]")
                ).otherwise(lit(None).cast(StringType()))
            )
        else:
            df = df.withColumn("institutions", lit(None).cast(StringType()))

        return df

    def _process_publication_info(self, df: DataFrame) -> DataFrame:
        """
        Process publication information.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with processed publication information
        """
        logger.info("Processing publication information")

        # Handle journal reference and publication status
        if "journal-ref" in df.columns:
            df = df.withColumn("journal_ref", col("journal-ref").cast(StringType()))
            df = df.withColumn("is_published", col("journal_ref").isNotNull())
        else:
            df = df.withColumn("journal_ref", lit(None).cast(StringType()))
            df = df.withColumn("is_published", lit(False))

        return df

    def _calculate_derived_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived metrics like update frequency and time to publication.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with calculated metrics
        """
        logger.info("Calculating derived metrics")

        # Calculate update frequency (average days between versions)
        df = df.withColumn(
            "update_frequency",
            when(
                (col("version_count") > 1) & col("submitted_date").isNotNull() & col("update_date").isNotNull(),
                datediff(col("update_date"), col("submitted_date")) / (col("version_count") - 1)
            ).otherwise(lit(None).cast(FloatType()))
        )

        # Calculate submission to publication time
        df = df.withColumn(
            "submission_to_publication",
            when(
                col("is_published") & col("submitted_date").isNotNull() & col("published_date").isNotNull(),
                datediff(col("published_date"), col("submitted_date"))
            ).otherwise(lit(None).cast(FloatType()))
        )

        return df

    def _create_partition_columns(self, df: DataFrame) -> DataFrame:
        """
        Create partition columns based on submitted date.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with partition columns
        """
        logger.info("Creating partition columns")

        # Create year, month, day partition columns from submitted_date
        df = df.withColumn(
            "year",
            when(
                col("submitted_date").isNotNull(),
                date_format(col("submitted_date"), "yyyy")
            ).otherwise(lit("unknown"))
        )

        df = df.withColumn(
            "month",
            when(
                col("submitted_date").isNotNull(),
                date_format(col("submitted_date"), "MM")
            ).otherwise(lit("00"))
        )

        df = df.withColumn(
            "day",
            when(
                col("submitted_date").isNotNull(),
                date_format(col("submitted_date"), "dd")
            ).otherwise(lit("00"))
        )

        return df

    def get_stats(self) -> Dict[str, Any]:
        """
        Get transformation statistics.

        Returns:
            Dictionary with transformation statistics
        """
        if self.stats["start_time"] and self.stats["end_time"]:
            self.stats["duration_seconds"] = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            logger.info(f"Transformation completed in {self.stats['duration_seconds']:.2f} seconds")
            logger.info(f"Processed {self.stats['input_records']} records: {self.stats['output_records']} successful, {self.stats['failed_records']} failed")

        return self.stats