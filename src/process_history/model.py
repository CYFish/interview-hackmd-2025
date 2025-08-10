import logging
from typing import Dict, Any
from datetime import datetime

from pyspark.sql import DataFrame
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, explode, split, expr


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ModelGenerator:
    """
    Responsible for creating relational data models from processed data.

    This class generates normalized data models for papers, authors, institutions,
    categories, and their relationships.
    """

    def __init__(self, glue_context, config: Dict[str, Any]):
        """
        Initialize the Glue data model generator.

        Args:
            glue_context: The GlueContext for the current job
            config: Configuration dictionary with model generation parameters
        """
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.config = config

        # Extract configuration
        self.output_path = config.get("output_path", "")
        self.bucket = config.get("bucket", "")
        self.database_name = config.get("database_name", "arxiv_data")

        # Full S3 path
        if self.bucket:
            self.s3_output_path = f"s3://{self.bucket}/{self.output_path}"
        else:
            self.s3_output_path = self.output_path

        # Statistics
        self.stats = {
            "total_papers": 0,
            "total_authors": 0,
            "total_institutions": 0,
            "total_categories": 0,
            "start_time": None,
            "end_time": None,
        }

    def generate_models(self, df: DataFrame) -> Dict[str, DataFrame]:
        """
        Generate relational data models from the processed data.

        Args:
            df: DataFrame with processed data

        Returns:
            Dictionary with generated data models
        """
        self.stats["start_time"] = datetime.now()
        self.stats["total_papers"] = df.count()
        logger.info(f"Generating data models for {self.stats['total_papers']} papers")

        try:
            # Create empty result dictionary
            models = {}

            # Generate paper_info model
            models["paper_info"] = self._generate_paper_info(df)

            # Generate authors model and paper_authors relationship
            authors_result = self._generate_authors(df)
            models["authors"] = authors_result["authors"]
            models["paper_authors"] = authors_result["paper_authors"]
            self.stats["total_authors"] = authors_result["author_count"]

            # Generate categories model and paper_categories relationship
            categories_result = self._generate_categories(df)
            models["categories"] = categories_result["categories"]
            models["paper_categories"] = categories_result["paper_categories"]
            self.stats["total_categories"] = categories_result["category_count"]

            # Generate institutions model and paper_institutions relationship
            institutions_result = self._generate_institutions(df)
            models["institutions"] = institutions_result["institutions"]
            models["paper_institutions"] = institutions_result["paper_institutions"]
            self.stats["total_institutions"] = institutions_result["institution_count"]

            logger.info(f"Generated {len(models)} data models")
            self.stats["end_time"] = datetime.now()
            return models

        except Exception as e:
            logger.error(f"Error generating data models: {e}")
            self.stats["end_time"] = datetime.now()
            raise

    def _generate_paper_info(self, df: DataFrame) -> DataFrame:
        """
        Generate the paper_info model.

        Args:
            df: DataFrame with processed data

        Returns:
            DataFrame with paper_info model
        """
        # Select relevant columns for paper_info
        paper_info = df.select(
            col("id"),
            col("title"),
            col("abstract"),
            col("submitted_date"),
            col("update_date"),
            col("published_date"),
            col("version_count"),
            col("journal_ref"),
            col("is_published"),
            col("doi"),
            col("update_frequency"),
            col("submission_to_publication"),
            col("year"),
            col("month"),
            col("day")
        )

        return paper_info

    def _generate_authors(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate the authors model and paper_authors relationship.

        Args:
            df: DataFrame with processed data

        Returns:
            Dictionary with authors model, paper_authors relationship, and author count
        """
        # Check if authors_parsed column exists and has the right format
        if "authors_parsed" not in df.columns:
            # Create empty DataFrames
            authors = self.spark.createDataFrame([], "author_id INT, last_name STRING, first_name STRING, middle_name STRING")
            paper_authors = self.spark.createDataFrame([], "paper_id STRING, author_id INT, author_position INT")
            return {"authors": authors, "paper_authors": paper_authors, "author_count": 0}

        # Explode the authors_parsed array to get one row per author
        exploded_df = df.select(
            col("id").alias("paper_id"),
            explode(col("authors_parsed")).alias("author_array")
        )

        # Extract author components and generate author_id
        authors_df = exploded_df.select(
            col("paper_id"),
            col("author_array").getItem(0).alias("last_name"),
            col("author_array").getItem(1).alias("first_name"),
            col("author_array").getItem(2).alias("middle_name")
        )

        # Create a unique ID for each author
        authors_df = authors_df.withColumn(
            "author_key",
            expr("md5(concat(last_name, '|', first_name, '|', middle_name))")
        )

        # Create paper_authors relationship with position
        paper_authors = authors_df.select(
            col("paper_id"),
            col("author_key"),
            expr("row_number() over (partition by paper_id order by 1)").alias("author_position")
        )

        # Create unique authors table
        unique_authors = authors_df.select(
            "author_key",
            "last_name",
            "first_name",
            "middle_name"
        ).distinct()

        # Add an integer author_id
        authors = unique_authors.withColumn(
            "author_id",
            expr("row_number() over (order by author_key)")
        ).select(
            "author_id",
            "last_name",
            "first_name",
            "middle_name"
        )

        # Join paper_authors with authors to get the integer author_id
        paper_authors = paper_authors.join(
            unique_authors.join(authors, ["last_name", "first_name", "middle_name"]).select("author_key", "author_id"),
            "author_key"
        ).select(
            "paper_id",
            "author_id",
            "author_position"
        )

        # Count unique authors
        author_count = authors.count()

        return {"authors": authors, "paper_authors": paper_authors, "author_count": author_count}

    def _generate_categories(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate the categories model and paper_categories relationship.

        Args:
            df: DataFrame with processed data

        Returns:
            Dictionary with categories model, paper_categories relationship, and category count
        """
        # Check if categories column exists
        if "categories" not in df.columns:
            # Create empty DataFrames
            categories = self.spark.createDataFrame([], "category_id INT, category_name STRING")
            paper_categories = self.spark.createDataFrame([], "paper_id STRING, category_id INT, is_primary BOOLEAN")
            return {"categories": categories, "paper_categories": paper_categories, "category_count": 0}

        # Explode categories into separate rows
        exploded_df = df.select(
            col("id").alias("paper_id"),
            explode(split(col("categories"), " ")).alias("category_name")
        )

        # Create unique categories table
        unique_categories = exploded_df.select("category_name").distinct()

        # Add an integer category_id
        categories = unique_categories.withColumn(
            "category_id",
            expr("row_number() over (order by category_name)")
        ).select(
            "category_id",
            "category_name"
        )

        # Create paper_categories relationship
        # Assume the first category is primary
        paper_categories = exploded_df.withColumn(
            "is_primary",
            expr("row_number() over (partition by paper_id order by 1) = 1")
        )

        # Join with categories to get category_id
        paper_categories = paper_categories.join(
            categories,
            "category_name"
        ).select(
            "paper_id",
            "category_id",
            "is_primary"
        )

        # Count unique categories
        category_count = categories.count()

        return {"categories": categories, "paper_categories": paper_categories, "category_count": category_count}

    def _generate_institutions(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate the institutions model and paper_institutions relationship.

        Args:
            df: DataFrame with processed data

        Returns:
            Dictionary with institutions model, paper_institutions relationship, and institution count
        """
        # Check if institutions column exists
        if "institutions" not in df.columns:
            # Create empty DataFrames
            institutions = self.spark.createDataFrame([], "institution_id INT, institution_name STRING")
            paper_institutions = self.spark.createDataFrame([], "paper_id STRING, institution_id INT")
            return {"institutions": institutions, "paper_institutions": paper_institutions, "institution_count": 0}

        # Explode institutions into separate rows
        # Assuming institutions are separated by semicolons
        exploded_df = df.select(
            col("id").alias("paper_id"),
            explode(split(col("institutions"), ";")).alias("institution_name")
        ).filter(col("institution_name") != "")

        # Create unique institutions table
        unique_institutions = exploded_df.select("institution_name").distinct()

        # Add an integer institution_id
        institutions = unique_institutions.withColumn(
            "institution_id",
            expr("row_number() over (order by institution_name)")
        ).select(
            "institution_id",
            "institution_name"
        )

        # Create paper_institutions relationship
        paper_institutions = exploded_df.join(
            institutions,
            "institution_name"
        ).select(
            "paper_id",
            "institution_id"
        )

        # Count unique institutions
        institution_count = institutions.count()

        return {"institutions": institutions, "paper_institutions": paper_institutions, "institution_count": institution_count}

    def write_models(self, models: Dict[str, DataFrame]) -> Dict[str, Any]:
        """
        Write generated data models to storage.

        Args:
            models: Dictionary with generated data models

        Returns:
            Dictionary with write operation results
        """
        if not models:
            logger.warning("No models to write")
            return {"status": "success", "tables_written": 0}

        try:
            tables_written = 0

            for model_name, df in models.items():
                if df.count() > 0:
                    # Convert to dynamic frame
                    dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, f"{model_name}_frame")

                    # Write to S3 with partitioning if applicable
                    partition_keys = ["year", "month", "day"] if model_name == "paper_info" else []

                    sink = self.glue_context.getSink(
                        path=f"{self.s3_output_path}/{model_name}",
                        connection_type="s3",
                        updateBehavior="UPDATE_IN_DATABASE",
                        partitionKeys=partition_keys,
                        compression="snappy",
                        enableUpdateCatalog=True,
                        transformation_ctx=f"write_{model_name}"
                    )

                    sink.setCatalogInfo(
                        catalogDatabase=self.database_name,
                        catalogTableName=model_name
                    )

                    sink.setFormat("glueparquet")
                    sink.writeFrame(dynamic_frame)

                    tables_written += 1
                    logger.info(f"Wrote {df.count()} records to {model_name} table")

            return {
                "status": "success",
                "tables_written": tables_written
            }

        except Exception as e:
            logger.error(f"Error writing data models: {e}")
            return {"status": "error", "error": str(e)}

    def get_stats(self) -> Dict[str, Any]:
        """
        Get model generation statistics.

        Returns:
            Dictionary with model generation statistics
        """
        if self.stats["start_time"] and self.stats["end_time"]:
            duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            self.stats["duration_seconds"] = duration

        return self.stats
