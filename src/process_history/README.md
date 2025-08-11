# ArXiv Historical Data Processor

This module processes historical ArXiv metadata from JSON files and converts them to Parquet format with partitioning by date.

## Features

- Flexible I/O options: read from local files or S3, write to local files or S3
- Partitioned output by year/month/day
- Configurable column ordering
- Batch processing for large datasets
- Data quality metrics and statistics

## Usage

```bash
# Process data with both input and output from/to S3
python main.py --input-path arxiv-metadata.json --output-path data/processed

# Read from local file, write to S3
python main.py --input-path data/raw/arxiv-metadata.json --output-path data/processed --input-local

# Read from S3, write to local file system
python main.py --input-path arxiv-metadata.json --output-path data/processed --output-local

# Read from local file, write to local file system
python main.py --input-path data/raw/arxiv-metadata.json --output-path data/processed --input-local --output-local

# Specify a different S3 bucket
python main.py --input-path arxiv-metadata.json --output-path data/processed --bucket my-bucket-name
```

## Command Line Arguments

- `--input-path`: Path to JSON file containing ArXiv data (local path or S3 key)
- `--output-path`: Path for output Parquet files (S3 prefix or local directory)
- `--input-local`: Read input from local file system instead of S3
- `--output-local`: Write output to local file system instead of S3
- `--bucket`: S3 bucket name (only used if not in local mode)
- `--chunk-size`: Number of records to process in each batch (default: 10000)

## Output Format

The data is written as Parquet files with a consistent column order, partitioned by date:

```
output_path/
  year=YYYY/
    month=MM/
      day=DD/
        part-00001.parquet
        part-00002.parquet
        ...
```
