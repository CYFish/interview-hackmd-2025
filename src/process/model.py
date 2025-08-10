import logging
from typing import Dict, List, Any


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ArxivDataModel:
    """
    Defines the data model for ArXiv papers.

    This class provides schema definitions and validation for
    ArXiv paper records stored in persistence systems.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the ArXiv data model.

        Args:
            config: Configuration dictionary with model parameters
        """
        self.config = config

    @staticmethod
    def get_schema() -> Dict[str, Any]:
        """
        Get the schema definition for ArXiv papers.

        Returns:
            Dictionary with schema definition
        """
        return {
            # Core paper metadata
            "paper_id": {"type": "string", "required": True},
            "title": {"type": "string"},
            "abstract": {"type": "string"},
            "categories": {"type": "list", "items": {"type": "string"}},
            "primary_category": {"type": "string"},
            "doi": {"type": "string"},

            # Temporal data
            "datestamp": {"type": "string"},
            "update_date": {"type": "string"},
            "first_submission_date": {"type": "string"},
            "last_update_date": {"type": "string"},
            "version_count": {"type": "number"},

            # Author and institution data
            "authors": {"type": "list", "items": {"type": "map"}},
            "authors_raw": {"type": "string"},
            "submitter": {"type": "string"},
            "institution": {"type": "string"},

            # Publication data
            "journal_ref": {"type": "string"},
            "is_published": {"type": "boolean"},

            # Version information
            "versions": {"type": "list", "items": {"type": "map"}},

            # Processing metadata
            "source_format": {"type": "string"},
            "last_processed": {"type": "string"}
        }

    def validate(self, record: Dict[str, Any]) -> List[str]:
        """
        Validate a record against the schema.

        Args:
            record: Record to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        schema = self.get_schema()

        # Check required fields
        for field, field_schema in schema.items():
            if field_schema.get("required", False) and field not in record:
                errors.append(f"Required field '{field}' is missing")

        # Check field types
        for field, value in record.items():
            if field in schema:
                field_schema = schema[field]
                field_type = field_schema.get("type")

                # Skip None values
                if value is None:
                    continue

                # Validate string fields
                if field_type == "string" and not isinstance(value, str):
                    errors.append(f"Field '{field}' should be a string, got {type(value).__name__}")

                # Validate number fields
                elif field_type == "number" and not isinstance(value, (int, float)):
                    errors.append(f"Field '{field}' should be a number, got {type(value).__name__}")

                # Validate boolean fields
                elif field_type == "boolean" and not isinstance(value, bool):
                    errors.append(f"Field '{field}' should be a boolean, got {type(value).__name__}")

                # Validate list fields
                elif field_type == "list" and not isinstance(value, list):
                    errors.append(f"Field '{field}' should be a list, got {type(value).__name__}")

                # Validate map fields
                elif field_type == "map" and not isinstance(value, dict):
                    errors.append(f"Field '{field}' should be a map, got {type(value).__name__}")

        return errors

    def get_query_examples(self) -> Dict[str, Any]:
        """
        Get examples of common queries for ArXiv papers.

        Returns:
            Dictionary with query examples
        """
        return {
            "get_paper_by_id": {
                "description": "Get a paper by its ID",
                "key_condition": {"paper_id": "1234.56789"},
                "table": "arxiv-papers"
            },
            "get_papers_by_category": {
                "description": "Get papers in a specific category",
                "index": "CategoryIndex",
                "key_condition": {"primary_category": "cs.AI"},
                "table": "arxiv-papers"
            },
            "get_papers_by_category_and_date_range": {
                "description": "Get papers in a specific category within a date range",
                "index": "CategoryIndex",
                "key_condition": {
                    "primary_category": "cs.AI",
                    "update_date": {"between": ["2023-01-01", "2023-12-31"]}
                },
                "table": "arxiv-papers"
            }
        }
