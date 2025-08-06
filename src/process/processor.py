import json
from typing import List, Dict, Any
from pathlib import Path

import boto3
import pandas as pd
import pendulum
import pyarrow.parquet as pq
import pyarrow as pa


class ArxivProcessor:
    """
    A processor for ArXiv metadata JSON files.

    Processes large JSON files containing ArXiv paper metadata,
    cleans the data, and converts it to Parquet format for efficient storage.
    """

    def __init__(self, input_path: str, output_path: str, chunk_size: int = 10000):
        """
        Initialize the ArXiv processor.

        Args:
            input_path: Path to the input JSON file
            output_path: Path for the output Parquet file
            chunk_size: Number of records to process in each batch
        """
        self.s3 = boto3.client("s3", region_name="ap-southeast-1")
        self.bucket = "hackmd-project-2025"
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.chunk_size = chunk_size
        self.buffers = {}
        self.partition_counters = {}
        self.is_first_batch = True
        self.schema = pa.schema([
            ("title", pa.string()),
            ("abstract", pa.string()),
            ("categories", pa.string()),
            ("doi", pa.string()),
            ("submitted_time", pa.timestamp("ns")),
            ("published_time", pa.timestamp("ns")),
            ("version_count", pa.int32()),
        ])

    def clean_data(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and transform a single JSON record.

        Args:
            record: Raw JSON record from ArXiv metadata

        Returns:
            Cleaned record with standardized fields
        """
        return {
            "title": record.get("title", "").strip(),
            "abstract": record.get("abstract", "").strip(),
            "categories": record.get("categories", ""),
            "doi": record.get("doi", ""),
            "submitted_time": pendulum.from_format(record.get("versions", [{}])[0].get("created", None), "ddd, D MMM YYYY HH:mm:ss z"),
            "published_time": pendulum.from_format(record.get("versions", [{}])[-1].get("created", None), "ddd, D MMM YYYY HH:mm:ss z"),
            "version_count": len(record.get("versions", [])),
        }

    def process(self) -> None:
        """
        Main processing pipeline: read, clean, and save data in batches.

        Reads the JSON file line by line, processes each record,
        and saves batches as Parquet files.
        """
        self.buffers = {}
        self.partition_counters = {}
        total_processed = 0

        print(f"Starting to process {self.input_path}")

        with self.input_path.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                try:
                    record = json.loads(line)
                    cleaned = self.clean_data(record)
                    submitted_date = cleaned["submitted_time"]
                    partition_key = f"year={submitted_date.year}/month={submitted_date.month:02}/day={submitted_date.day:02}"

                    if partition_key not in self.buffers:
                        self.buffers[partition_key] = []
                    self.buffers[partition_key].append(cleaned)

                    if len(self.buffers[partition_key]) >= self.chunk_size:
                        self._write_parquet(partition_key, self.buffers[partition_key])
                        total_processed += len(self.buffers[partition_key])
                        print(f"Processed {i + 1} records so far")
                        self.buffers.pop(partition_key)

                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON at line {i + 1}: {e}")
                    continue

                if i == 10:
                    break

            # Process the remaining batch
            for partition_key, cleaned_records in list(self.buffers.items()):
                self._write_parquet(partition_key, cleaned_records)

        print(f"Processing complete. Total records processed: {total_processed}")

    def _write_parquet(self, partition_key: str, batch: List[Dict[str, Any]]) -> None:
        """
        Write a batch of records to Parquet format.

        Args:
            batch: List of cleaned records
            count: Current record count for progress tracking
        """
        df = pd.DataFrame(batch)

        if partition_key not in self.partition_counters:
            self.partition_counters[partition_key] = 1
        else:
            self.partition_counters[partition_key] += 1

        # Show sample data for the first batch
        if self.is_first_batch:
            print("\n=== Sample data from first batch ===")
            sample_data = df[["submitted_time", "published_time", "version_count"]]
            print(sample_data.head())
            print(f"Columns: {list(sample_data.columns)}")
            print("=" * 50)

            self.is_first_batch = False

        table = pa.Table.from_pandas(df, schema=self.schema)

        partition_path = self.output_path / partition_key
        partition_path.mkdir(parents=True, exist_ok=True)
        part_file = partition_path / f"part_{self.partition_counters[partition_key]:04}.parquet"
        pq.write_table(table, part_file)
        print(f"✓ Wrote {part_file}")


def main():
    """Main function to run the ArXiv processor."""
    processor = ArxivProcessor(
        input_path="data/arxiv-metadata-oai-snapshot.json",
        output_path="data/processed/",
        chunk_size=2,
    )
    processor.process()


if __name__ == "__main__":
    main()
