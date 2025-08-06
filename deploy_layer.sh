#!/bin/bash

# Configuration
ZIP_FILE_NAME="dependencies.zip"
LAYER_NAME="process-layer"
RUNTIME="python3.13"
BUCKET="hackmd-project-2025"
OBJECT_PREFIX="layers"

echo "🚀 Deploying Lambda Layer"
echo "========================="

# Check if layer file exists
if [ ! -f "$ZIP_FILE_NAME" ]; then
    echo "Creating $ZIP_FILE_NAME first..."
    zip -r $ZIP_FILE_NAME .venv/lib/python3.13/site-packages/
fi

# Upload the zip file to S3
if ! aws s3 ls s3://$BUCKET/$OBJECT_PREFIX/$ZIP_FILE_NAME > /dev/null 2>&1; then
    echo "📤 Uploading dependencies: $ZIP_FILE_NAME"
    aws s3 cp $ZIP_FILE_NAME s3://$BUCKET/$OBJECT_PREFIX
fi

# Publish the layer
echo "📦 Publishing layer: $LAYER_NAME"
echo "🐍 Runtime: $RUNTIME"
aws lambda publish-layer-version \
    --layer-name $LAYER_NAME \
    --content S3Bucket=$BUCKET,S3Key=$OBJECT_PREFIX/$ZIP_FILE_NAME \
    --compatible-runtimes $RUNTIME

if [ $? -eq 0 ]; then
    echo "✅ Layer published successfully!"
    echo ""
    echo "📋 Next steps:"
    echo "1. Go to AWS Lambda Console"
    echo "2. Create or update your Lambda function"
    echo "3. Add the layer: $LAYER_NAME"
    echo "4. Set handler to: lambda_function.lambda_handler"
else
    echo "❌ Failed to publish layer"
    exit 1
fi