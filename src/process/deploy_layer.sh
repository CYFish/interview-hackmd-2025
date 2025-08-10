#!/bin/bash

# Configuration
ZIP_FILE_NAME=dependencies.zip
LAYER_NAME=processor-layer
RUNTIME=python3.13
ARCHITECTURE=x86_64
BUCKET=hackmd-project-2025
OBJECT_PREFIX=layers

echo "========================================"
echo "🚀 Deploying Lambda Layer"
echo "========================================"

# Check if layer file exists
if [ ! -f "$ZIP_FILE_NAME" ]; then
    echo "📦 Creating $ZIP_FILE_NAME first..."

    # Install with size optimization options
    pip install \
        --platform manylinux2014_x86_64 \
        --python-version 3.13 \
        --only-binary=:all: \
        --no-compile \
        --no-cache-dir \
        --target python \
        -r src/process/requirements.txt
    if [ $? -ne 0 ]; then
        echo "❌ Failed to install dependencies"
        exit 1
    fi

    # Quick cleanup of obvious bloat
    echo "🧹 Removing unnecessary files..."
    find python -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    find python -name "*.pyc" -delete 2>/dev/null || true
    find python -name "tests" -type d -exec rm -rf {} + 2>/dev/null || true
    find python -name "test" -type d -exec rm -rf {} + 2>/dev/null || true

    echo "📊 Package size after cleanup:"
    du -sh python/

    zip -rq $ZIP_FILE_NAME python/

    echo "📊 ZIP file size:"
    ls -lh $ZIP_FILE_NAME
fi

# Upload the zip file to S3
echo "📤 Uploading dependencies: $ZIP_FILE_NAME"
aws s3 cp $ZIP_FILE_NAME s3://$BUCKET/$OBJECT_PREFIX/$ZIP_FILE_NAME
echo "🧹 Cleaning up the files"
rm -r python/
rm $ZIP_FILE_NAME

# Publish the layer
echo "📤 Publishing layer: $LAYER_NAME"
echo "🐍 Runtime: $RUNTIME"
aws lambda publish-layer-version \
    --layer-name $LAYER_NAME \
    --content S3Bucket=$BUCKET,S3Key=$OBJECT_PREFIX/$ZIP_FILE_NAME \
    --compatible-runtimes $RUNTIME \
    --compatible-architectures "$ARCHITECTURE"

if [ $? -eq 0 ]; then
    echo "✅ Layer published successfully!"
else
    echo "❌ Failed to publish layer"
    exit 1
fi
