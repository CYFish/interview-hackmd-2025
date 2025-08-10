import logging
import json
from typing import Dict, List, Any, Optional
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Helper class to convert Decimal to float for JSON serialization
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


class DataQueryService:
    """
    Provides query functionality for ArXiv data stored in persistence layer.

    This class implements various query patterns to support analytical
    use cases such as dashboard metrics and paper recommendations.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the data query service.

        Args:
            config: Configuration dictionary with query parameters
        """
        self.config = config
        self.region_name = config.get("region_name", "ap-northeast-1")
        self.table_name = config.get("table_name", "arxiv-papers")

        # Initialize DynamoDB resources
        self.dynamodb = boto3.resource("dynamodb", region_name=self.region_name)
        self.table = self.dynamodb.Table(self.table_name)

    def get_paper_by_id(self, paper_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a paper by its ID.

        Args:
            paper_id: Paper ID to retrieve

        Returns:
            Paper record or None if not found
        """
        try:
            response = self.table.get_item(
                Key={"paper_id": paper_id}
            )

            item = response.get("Item")
            if item:
                return json.loads(json.dumps(item, cls=DecimalEncoder))
            return None

        except ClientError as e:
            logger.error(f"Error getting paper {paper_id}: {e}")
            return None

    def get_papers_by_category(self, category: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get papers in a specific category.

        Args:
            category: Category code (e.g., "cs.AI")
            limit: Maximum number of results to return

        Returns:
            List of paper records
        """
        try:
            response = self.table.query(
                IndexName="CategoryIndex",
                KeyConditionExpression=Key("primary_category").eq(category),
                Limit=limit
            )

            items = response.get("Items", [])
            return json.loads(json.dumps(items, cls=DecimalEncoder))

        except ClientError as e:
            logger.error(f"Error querying papers by category {category}: {e}")
            return []

    def get_papers_by_date_range(self, start_date: str, end_date: str,
                                 category: Optional[str] = None, limit: int = 100,
                                 sort_by: str = "update_date") -> List[Dict[str, Any]]:
        """
        Get papers within a date range, optionally filtered by category.

        Args:
            start_date: Start date in ISO format (YYYY-MM-DD)
            end_date: End date in ISO format (YYYY-MM-DD)
            category: Optional category code filter
            limit: Maximum number of results to return
            sort_by: Field to sort by (default: update_date)

        Returns:
            List of paper records
        """
        try:
            if category:
                # Query by category and date range using the index
                response = self.table.query(
                    IndexName="CategoryIndex",
                    KeyConditionExpression=
                        Key("primary_category").eq(category) &
                        Key("update_date").between(start_date, end_date),
                    Limit=limit
                )
            else:
                # Scan with date range filter (less efficient)
                response = self.table.scan(
                    FilterExpression=
                        Attr("update_date").between(start_date, end_date),
                    Limit=limit
                )

            items = response.get("Items", [])
            return json.loads(json.dumps(items, cls=DecimalEncoder))

        except ClientError as e:
            logger.error(f"Error querying papers by date range: {e}")
            return []

    def get_average_updates_by_category(self) -> Dict[str, Any]:
        """
        Calculate average number of updates per paper by category.

        Returns:
            Dictionary mapping categories to average update counts
        """
        try:
            # Scan the table to get all papers with version_count
            response = self.table.scan(
                ProjectionExpression="primary_category, version_count"
            )

            items = response.get("Items", [])

            # Group by category
            categories = {}
            for item in items:
                category = item.get("primary_category")
                version_count = item.get("version_count")

                if not category or not version_count:
                    continue

                if category not in categories:
                    categories[category] = {
                        "total_papers": 0,
                        "total_updates": 0
                    }

                categories[category]["total_papers"] += 1
                categories[category]["total_updates"] += version_count

            # Calculate averages
            result = {}
            for category, data in categories.items():
                if data["total_papers"] > 0:
                    result[category] = {
                        "average_updates": data["total_updates"] / data["total_papers"],
                        "total_papers": data["total_papers"]
                    }

            # Convert Decimal to float for JSON serialization
            return json.loads(json.dumps(result, cls=DecimalEncoder))

        except ClientError as e:
            logger.error(f"Error calculating average updates: {e}")
            return {}

    def get_submission_to_publication_time(self, category: Optional[str] = None) -> Dict[str, Any]:
        """
        Calculate average time from submission to publication by category.

        Args:
            category: Optional category code filter

        Returns:
            Dictionary with submission to publication time metrics
        """
        try:
            # Prepare filter expression
            filter_expr = Attr("is_published").eq(True) & Attr("submission_to_publication").exists()
            if category:
                filter_expr = filter_expr & Attr("primary_category").eq(category)

            # Scan the table for published papers with submission_to_publication data
            response = self.table.scan(
                FilterExpression=filter_expr,
                ProjectionExpression="paper_id, primary_category, submission_to_publication, submitted_date, published_date, journal_ref"
            )

            items = response.get("Items", [])

            # Group by category
            categories = {}
            for item in items:
                cat = item.get("primary_category")
                if not cat:
                    continue

                if cat not in categories:
                    categories[cat] = {
                        "total_days": 0,
                        "count": 0
                    }

                # Get submission_to_publication value
                days = item.get("submission_to_publication")
                if days is not None:
                    categories[cat]["total_days"] += days
                    categories[cat]["count"] += 1

            # Calculate metrics
            result = {}
            for cat, data in categories.items():
                if data["count"] > 0:
                    avg_days = data["total_days"] / data["count"]

                    result[cat] = {
                        "average_days": avg_days,
                        "sample_size": data["count"]
                    }

            # Convert Decimal to float for JSON serialization
            return json.loads(json.dumps(result, cls=DecimalEncoder))

        except ClientError as e:
            logger.error(f"Error calculating submission to publication time: {e}")
            return {}

    def get_papers_by_author(self, author_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get papers by author name (partial match on first or last name).

        Args:
            author_name: Author name to search for
            limit: Maximum number of results to return

        Returns:
            List of paper records
        """
        try:
            # Scan is inefficient but necessary for this query pattern
            # In a production system, consider using a secondary index or search service
            response = self.table.scan(
                FilterExpression=Attr("authors").exists(),
                Limit=limit
            )

            items = response.get("Items", [])
            author_name_lower = author_name.lower()

            # Filter papers by author name
            matching_papers = []
            for item in items:
                authors = item.get("authors", [])
                for author in authors:
                    first_name = author.get("first_name", "").lower()
                    last_name = author.get("last_name", "").lower()

                    if (author_name_lower in first_name) or (author_name_lower in last_name):
                        matching_papers.append(item)
                        break

                if len(matching_papers) >= limit:
                    break

            return json.loads(json.dumps(matching_papers, cls=DecimalEncoder))

        except ClientError as e:
            logger.error(f"Error querying papers by author: {e}")
            return []

    def get_papers_by_update_frequency(self, min_frequency: float = None, max_frequency: float = None,
                                      limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get papers filtered by update frequency (in days).

        Args:
            min_frequency: Minimum update frequency in days
            max_frequency: Maximum update frequency in days
            limit: Maximum number of results to return

        Returns:
            List of paper records
        """
        try:
            filter_expr = Attr("update_frequency").exists()

            if min_frequency is not None:
                filter_expr = filter_expr & Attr("update_frequency").gte(min_frequency)

            if max_frequency is not None:
                filter_expr = filter_expr & Attr("update_frequency").lte(max_frequency)

            response = self.table.scan(
                FilterExpression=filter_expr,
                Limit=limit
            )

            items = response.get("Items", [])
            return json.loads(json.dumps(items, cls=DecimalEncoder))

        except ClientError as e:
            logger.error(f"Error querying papers by update frequency: {e}")
            return []

    def get_paper_versions(self, paper_id: str) -> List[Dict[str, Any]]:
        """
        Get all versions of a specific paper.

        Args:
            paper_id: Paper ID to retrieve versions for

        Returns:
            List of paper versions or empty list if not found
        """
        try:
            response = self.table.get_item(
                Key={"paper_id": paper_id},
                ProjectionExpression="paper_id, title, versions"
            )

            item = response.get("Item")
            if not item or "versions" not in item:
                return []

            versions = item.get("versions", [])
            return json.loads(json.dumps(versions, cls=DecimalEncoder))

        except ClientError as e:
            logger.error(f"Error getting versions for paper {paper_id}: {e}")
            return []

    def get_institution_submission_counts(self, limit: int = 50) -> Dict[str, int]:
        """
        Get submission counts by institution.

        Args:
            limit: Maximum number of institutions to return

        Returns:
            Dictionary mapping institutions to submission counts
        """
        try:
            # Scan the table to get all papers with institution
            response = self.table.scan(
                ProjectionExpression="institution",
                FilterExpression=Attr("institution").exists()
            )

            items = response.get("Items", [])

            # Count submissions by institution
            institutions = {}
            for item in items:
                institution = item.get("institution")
                if not institution:
                    continue

                institutions[institution] = institutions.get(institution, 0) + 1

            # Sort by count and limit
            sorted_institutions = dict(sorted(
                institutions.items(),
                key=lambda x: x[1],
                reverse=True
            )[:limit])

            # Convert Decimal to float for JSON serialization
            return json.loads(json.dumps(sorted_institutions, cls=DecimalEncoder))

        except ClientError as e:
            logger.error(f"Error getting institution submission counts: {e}")
            return {}


def main():
    """
    Command-line interface for data queries.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Query ArXiv data from persistence layer")
    parser.add_argument("--region", type=str, default="ap-northeast-1",
                        help="AWS region")
    parser.add_argument("--table", type=str, default="arxiv-papers",
                        help="DynamoDB table name")
    parser.add_argument("--query", type=str, required=True, choices=[
                        "paper", "category", "date-range", "updates", "publication-time",
                        "institutions", "author", "versions", "update-frequency"],
                        help="Query type")
    parser.add_argument("--paper-id", type=str,
                        help="Paper ID for paper query or versions query")
    parser.add_argument("--category", type=str,
                        help="Category for category query")
    parser.add_argument("--start-date", type=str,
                        help="Start date for date range query (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str,
                        help="End date for date range query (YYYY-MM-DD)")
    parser.add_argument("--author", type=str,
                        help="Author name for author query")
    parser.add_argument("--min-frequency", type=float,
                        help="Minimum update frequency in days")
    parser.add_argument("--max-frequency", type=float,
                        help="Maximum update frequency in days")
    parser.add_argument("--sort-by", type=str, default="update_date",
                        help="Field to sort results by")
    parser.add_argument("--limit", type=int, default=100,
                        help="Maximum number of results")
    parser.add_argument("--output", type=str,
                        help="Output file path")

    args = parser.parse_args()

    # Create query service
    config = {
        "region_name": args.region,
        "table_name": args.table
    }
    query_service = DataQueryService(config)

    # Execute query
    result = None
    if args.query == "paper":
        if not args.paper_id:
            print("Error: --paper-id is required for paper query")
            return
        result = query_service.get_paper_by_id(args.paper_id)

    elif args.query == "category":
        if not args.category:
            print("Error: --category is required for category query")
            return
        result = query_service.get_papers_by_category(args.category, args.limit)

    elif args.query == "date-range":
        if not args.start_date or not args.end_date:
            print("Error: --start-date and --end-date are required for date range query")
            return
        result = query_service.get_papers_by_date_range(args.start_date, args.end_date, args.category, args.limit, args.sort_by)

    elif args.query == "updates":
        result = query_service.get_average_updates_by_category()

    elif args.query == "publication-time":
        result = query_service.get_submission_to_publication_time(args.category)

    elif args.query == "author":
        if not args.author:
            print("Error: --author is required for author query")
            return
        result = query_service.get_papers_by_author(args.author, args.limit)

    elif args.query == "versions":
        if not args.paper_id:
            print("Error: --paper-id is required for versions query")
            return
        result = query_service.get_paper_versions(args.paper_id)

    elif args.query == "update-frequency":
        result = query_service.get_papers_by_update_frequency(
            args.min_frequency, args.max_frequency, args.limit)

    elif args.query == "institutions":
        result = query_service.get_institution_submission_counts(args.limit)

    # Output result
    if result:
        if args.output:
            with open(args.output, "w") as f:
                json.dump(result, f, indent=2)
            print(f"Results written to {args.output}")
        else:
            print(json.dumps(result, indent=2))
    else:
        print("No results found")


if __name__ == "__main__":
    main()
