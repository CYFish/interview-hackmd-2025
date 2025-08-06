import json
import logging

from typing import List, Dict, Any

import boto3
import pandas as pd
import pendulum
import pyarrow.parquet as pq
import pyarrow as pa


logging.basicConfig(level=logging.INFO)

class ArxivProcessor:
    """
    A processor for ArXiv metadata JSON files.

    Processes large JSON files containing ArXiv paper metadata,
    cleans the data, and converts it to Parquet format for efficient storage.
    """

    def __init__(self, input_prefix: str, output_prefix: str, chunk_size: int = 10000):
        """
        Initialize the ArXiv processor.

        Args:
            input_path: Path to the input JSON file
            output_path: Path for the output Parquet file
            chunk_size: Number of records to process in each batch
        """
        self.s3 = boto3.client("s3", region_name="ap-northeast-1")
        self.bucket = "hackmd-project-2025"
        self.input_path = input_prefix
        self.output_path = output_prefix
        self.chunk_size = chunk_size
        self.buffers = {}
        self.counters = {}
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
        total_processed = 0

        logging.info(f"Starting to process s3://{self.bucket}/{self.input_path}")
        file_object = self.s3.get_object(Bucket=self.bucket, Key=self.input_path)
        raw_lines = file_object["Body"].iter_lines()
        for i, line in enumerate(raw_lines):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                cleaned = self.clean_data(record)

                submitted_time = cleaned["submitted_time"]
                partition_key = f"year={submitted_time.year}/month={submitted_time.month:02}/day={submitted_time.day:02}"

                if partition_key not in self.buffers:
                    self.buffers[partition_key] = []
                    self.counters[partition_key] = 0

                self.buffers[partition_key].append(cleaned)

                if len(self.buffers[partition_key]) >= self.chunk_size:
                    self._write_parquet(partition_key, self.buffers[partition_key])
                    total_processed += len(self.buffers[partition_key])
                    logging.info(f"Processed {i + 1} records so far")
                    self.buffers[partition_key] = []

            except json.JSONDecodeError as e:
                logging.error(f"Error parsing JSON at line {i + 1}: {e}")
                continue

            if i == 9:
                break

        # Process the remaining batch
        for partition_key, cleaned_records in list(self.buffers.items()):
            if cleaned_records:
                self._write_parquet(partition_key, cleaned_records)
                total_processed += len(self.buffers[partition_key])

        logging.info(f"Processing complete. Total records processed: {total_processed}")

    def _write_parquet(self, partition_key: str, batch: List[Dict[str, Any]]) -> None:
        """
        Write a batch of records to Parquet format.

        Args:
            batch: List of cleaned records
            count: Current record count for progress tracking
        """
        df = pd.DataFrame(batch)

        if partition_key not in self.counters:
            self.counters[partition_key] = 1
        else:
            self.counters[partition_key] += 1

        table = pa.Table.from_pandas(df, schema=self.schema)

        # part_file = f"s3://{self.bucket}/{self.output_path}{partition_key}/part_{self.counters[partition_key]:04}.parquet"
        partition_path = f"s3://{self.bucket}/{self.output_path}{partition_key}"
        pq.write_to_dataset(table, partition_path)
        logging.info(f"✓ Wrote {partition_path}")


def main():
    """Main function to run the ArXiv processor."""
    processor = ArxivProcessor(
        input_prefix="raw/initial/arxiv-metadata-oai-snapshot.json",
        output_prefix="processed/",
        chunk_size=2,
    )
    processor.process()


if __name__ == "__main__":
    main()
