#!/bin/bash
set -e

# AWS Glue deployment script for process-glue
BUCKET_NAME="hackmd-project-2025"
REGION="ap-northeast-1"
DATABASE_NAME="arxiv_history"
ROLE_NAME="hackmd-glue"
JOB_NAME="arxiv-processor"
GLUE_VERSION="5.0"
WORKER_TYPE="G.1X"
WORKER_COUNT=5
TIMEOUT=120  # 2 hours

echo "========================================"
echo "🚀 Deploying Glue Job: $JOB_NAME"
echo "========================================"

# Create a directory for packaging
echo "📦 Creating deployment package..."
TEMP_DIR=$(mktemp -d)
mkdir -p "$TEMP_DIR"

# Copy Python scripts to the temp directory
echo "📄 Copying Python modules..."
cp src/process_history/__init__.py "$TEMP_DIR/"
cp src/process_history/extractor.py "$TEMP_DIR/"
cp src/process_history/main.py "$TEMP_DIR/"
cp src/process_history/metric.py "$TEMP_DIR/"
cp src/process_history/model.py "$TEMP_DIR/"
cp src/process_history/pipeline.py "$TEMP_DIR/"
cp src/process_history/quality.py "$TEMP_DIR/"
cp src/process_history/transformer.py "$TEMP_DIR/"
cp src/process_history/writer.py "$TEMP_DIR/"

# Get the role ARN
ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query "Role.Arn" --output text 2>/dev/null || echo "")

# Create role if it doesn't exist
if [ -z "$ROLE_ARN" ]; then
    echo "🔑 Creating IAM role: $ROLE_NAME"

    # Create trust policy document
    cat > "$TEMP_DIR/trust-policy.json" << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

    # Create the role
    ROLE_ARN=$(aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document file://"$TEMP_DIR/trust-policy.json" \
        --query "Role.Arn" \
        --output text)

    # Attach policies
    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"

    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonS3FullAccess"

    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/CloudWatchFullAccess"

    echo "✅ Role created: $ROLE_ARN"
else
    echo "📁 Using existing role: $ROLE_ARN"
fi

# Create a zip file with the scripts
echo "📦 Zipping script files..."
(cd "$TEMP_DIR" && zip -r scripts.zip .)

# Upload all files to S3
echo "📤 Uploading files to S3..."
# Upload both main script and scripts package
UPLOAD_OUTPUT=$(aws s3 cp "src/process_history/main.py" "s3://$BUCKET_NAME/glue-scripts/main.py" && aws s3 cp "$TEMP_DIR/scripts.zip" "s3://$BUCKET_NAME/glue-scripts/scripts.zip" 2>&1)
if [ $? -eq 0 ]; then
    echo "✅ All files uploaded successfully"
else
    echo "❌ Error: Failed to upload files"
    echo "Error details: $UPLOAD_OUTPUT"
    exit 1
fi

# Create Glue database if it doesn't exist
echo "🏗️ Creating Glue database if it doesn't exist..."
aws glue create-database --database-input "{\"Name\":\"$DATABASE_NAME\"}" 2>/dev/null || true

# Check if the Glue job already exists
echo "🔍 Checking if Glue job $JOB_NAME exists..."
if aws glue get-job --job-name "$JOB_NAME" 2>/dev/null; then
    echo "🔄 Updating existing Glue job: $JOB_NAME"

    # Update the job
    UPDATE_JOB_OUTPUT=$(aws glue update-job \
        --job-name "$JOB_NAME" \
        --job-update "{
            \"Role\": \"$ROLE_ARN\",
            \"ExecutionProperty\": {
                \"MaxConcurrentRuns\": 1
            },
            \"Command\": {
                \"Name\": \"glueetl\",
                \"ScriptLocation\": \"s3://$BUCKET_NAME/glue-scripts/main.py\",
                \"PythonVersion\": \"3\"
            },
            \"DefaultArguments\": {
                \"--job-language\": \"python\",
                \"--class\": \"GlueJob\",
                \"--database_name\": \"$DATABASE_NAME\",
                \"--table_name\": \"processed_papers\",
                \"--input_path\": \"raw/initial/arxiv-metadata-oai-snapshot.json\",
                \"--output_path\": \"processed/\",
                \"--bucket\": \"$BUCKET_NAME\",
                \"--enable-metrics\": \"true\",
                \"--enable-continuous-cloudwatch-log\": \"true\",
                \"--extra-py-files\": \"s3://$BUCKET_NAME/glue-scripts/scripts.zip\",
                \"--generate_models\": \"false\",
                \"--write_models\": \"false\",
                \"--use_cloudwatch\": \"true\",
                \"--update_mode\": \"merge\",
                \"--region\": \"$REGION\"
            },
            \"MaxRetries\": 2,
            \"Timeout\": $TIMEOUT,
            \"GlueVersion\": \"$GLUE_VERSION\",
            \"NumberOfWorkers\": $WORKER_COUNT,
            \"WorkerType\": \"$WORKER_TYPE\"
        }" 2>&1)

    if [ $? -eq 0 ]; then
        echo "✅ Glue job updated successfully"
    else
        echo "❌ Error: Failed to update Glue job"
        echo "Error details: $UPDATE_JOB_OUTPUT"
        exit 1
    fi
else
    echo "🆕 Creating new Glue job: $JOB_NAME"

    # Create a new job
    CREATE_JOB_OUTPUT=$(aws glue create-job \
        --name "$JOB_NAME" \
        --role "$ROLE_ARN" \
        --execution-property "{\"MaxConcurrentRuns\": 1}" \
        --command "{\"Name\": \"glueetl\", \"ScriptLocation\": \"s3://$BUCKET_NAME/glue-scripts/main.py\", \"PythonVersion\": \"3\"}" \
        --default-arguments "{
            \"--job-language\": \"python\",
            \"--class\": \"GlueJob\",
            \"--database_name\": \"$DATABASE_NAME\",
            \"--table_name\": \"processed_papers\",
            \"--input_path\": \"raw/initial/arxiv-metadata-oai-snapshot.json\",
            \"--output_path\": \"processed/\",
            \"--bucket\": \"$BUCKET_NAME\",
            \"--enable-metrics\": \"true\",
            \"--enable-continuous-cloudwatch-log\": \"true\",
            \"--extra-py-files\": \"s3://$BUCKET_NAME/glue-scripts/scripts.zip\",
            \"--generate_models\": \"false\",
            \"--write_models\": \"false\",
            \"--use_cloudwatch\": \"true\",
            \"--update_mode\": \"merge\",
            \"--region\": \"$REGION\"
        }" \
        --max-retries 2 \
        --timeout $TIMEOUT \
        --glue-version "$GLUE_VERSION" \
        --number-of-workers $WORKER_COUNT \
        --worker-type "$WORKER_TYPE" \
        --region "$REGION" 2>&1)

    if [ $? -eq 0 ]; then
        echo "✅ Glue job created successfully"
    else
        echo "❌ Error: Failed to create Glue job"
        echo "Error details: $CREATE_JOB_OUTPUT"
        exit 1
    fi
fi

# Clean up
echo "🧹 Cleaning up..."
rm -rf "$TEMP_DIR"

# Create test event file if it doesn't exist
TEST_EVENT_FILE="src/process_history/test_event_$JOB_NAME.json"
if [ ! -f "$TEST_EVENT_FILE" ]; then
    echo "📝 Creating test event file: $TEST_EVENT_FILE"
    cat > $TEST_EVENT_FILE << EOF
{
    "--input_path": "raw/initial/arxiv-metadata-oai-snapshot.json",
    "--output_path": "processed/",
    "--database_name": "$DATABASE_NAME",
    "--table_name": "processed_papers",
    "--generate_models": "true",
    "--write_models": "true",
    "--use_cloudwatch": "true",
    "--region": "$REGION"
}
EOF
    echo "✅ Test event file created"
fi

echo "🎉 Glue deployment completed successfully!"
echo "Job name: $JOB_NAME"
echo "Glue version: $GLUE_VERSION"
echo "Worker type: $WORKER_TYPE"
echo "Worker count: $WORKER_COUNT"
echo "Timeout: ${TIMEOUT}m"
echo "Database: $DATABASE_NAME"
echo "S3 bucket: $BUCKET_NAME"
echo ""
echo "To run your Glue job, use:"
echo "aws glue start-job-run --job-name $JOB_NAME --arguments file://$TEST_EVENT_FILE"
