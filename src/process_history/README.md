# AWS Glue Implementation for ArXiv Data Processing

This directory contains the AWS Glue implementation for processing ArXiv metadata. The Glue-based approach offers improved scalability, better handling of large datasets, and native integration with AWS data catalog compared to the Lambda-based implementation.

## Architecture

The implementation follows a modular architecture with the following components:

1. **ArxivGluePipeline**: Main orchestrator that coordinates the entire data processing workflow
2. **GlueDataExtractor**: Responsible for extracting data from source locations (S3)
3. **GlueDataTransformer**: Handles data cleaning, normalization, and feature extraction
4. **GlueDataQualityChecker**: Performs data quality checks and generates quality reports
5. **GlueDataWriter**: Writes processed data to destination storage (S3)
6. **GlueMetricsCollector**: Collects and publishes processing metrics for monitoring
7. **GlueDataModelGenerator**: Creates relational data models from the processed data

The overall flow is:

```
Raw Data (S3) → DataExtractor → DataTransformer → DataQualityChecker → DataWriter → Processed Data (S3) → Data Catalog
                    ↓                                                      ↓
              DataModelGenerator                                    MetricsCollector
                    ↓                                                      ↓
              Relational Models                                      CloudWatch Metrics
```

## Data Models

The pipeline can optionally generate the following relational data models:

1. **paper_info**: Core paper metadata
2. **authors**: Author information
3. **paper_authors**: Paper-author relationships
4. **institutions**: Institution information
5. **paper_institutions**: Paper-institution relationships
6. **categories**: Category/subject area information
7. **paper_categories**: Paper-category relationships

## Deployment

To deploy the Glue job:

1. Make the deployment script executable:
   ```
   chmod +x deploy_glue.sh
   ```

2. Run the deployment script:
   ```
   ./deploy_glue.sh [bucket-name] [region] [database-name] [role-name] [job-name]
   ```

   Default values:
   - bucket-name: hackmd-project-2025
   - region: ap-northeast-1
   - database-name: arxiv_data
   - role-name: GlueArxivProcessorRole
   - job-name: arxiv-processor

3. The script will:
   - Create an S3 bucket if it doesn't exist
   - Create an IAM role with necessary permissions
   - Upload the Glue scripts to S3
   - Create a Glue database
   - Create or update the Glue job

## Running the Glue Job

You can run the Glue job using the AWS Management Console or the AWS CLI:

```bash
aws glue start-job-run --job-name arxiv-processor --arguments '{"--generate_models":"true"}'
```

## Job Parameters

The Glue job accepts the following parameters:

- `--database_name`: The name of the Glue database (default: arxiv_data)
- `--table_name`: The name of the output table (default: processed_papers)
- `--input_path`: The S3 path for input data (default: raw/)
- `--output_path`: The S3 path for output data (default: processed/)
- `--bucket`: The S3 bucket name
- `--generate_models`: Whether to generate relational data models (default: false)
- `--write_models`: Whether to write generated models to S3 (default: false)
- `--use_cloudwatch`: Whether to publish metrics to CloudWatch (default: false)
- `--update_mode`: How to handle updates to existing data (merge, overwrite, append) (default: merge)
- `--region`: AWS region (default: ap-northeast-1)

## Data Schema

The processed data follows this schema:

- **Core paper metadata**: id, title, abstract, categories, doi
- **Temporal data**: submitted_date, update_date, published_date, version_count
- **Author and institution data**: authors_parsed, institutions
- **Publication data**: journal_ref, is_published
- **Additional fields**: update_frequency, submission_to_publication
- **Partition columns**: year, month, day

## Monitoring

The Glue job publishes metrics to CloudWatch, which can be used to monitor:

- Job execution time
- Records processed
- Success/failure rates
- Data quality metrics
- Resource utilization

## Querying the Data

After the job runs successfully, you can query the processed data using:

- AWS Athena
- AWS Glue DataBrew
- Any service that integrates with the AWS Glue Data Catalog

Example Athena query:

```sql
SELECT
  id,
  title,
  categories,
  submitted_date,
  is_published
FROM
  arxiv_data.processed_papers
WHERE
  year = '2023'
  AND month = '01'
LIMIT 10;
```

## Key Benefits Over Lambda

- **Scalability**: Glue can handle much larger datasets than Lambda
- **Processing Time**: No 15-minute timeout limitation (Glue jobs can run for hours)
- **Memory**: Access to more memory and compute resources
- **Native Partitioning**: Built-in support for partitioning data
- **Data Catalog Integration**: Automatic schema discovery and catalog updates
- **Cost Efficiency**: More cost-effective for large-scale batch processing
- **Monitoring**: Better integration with CloudWatch for monitoring and metrics