#!/bin/bash

# AWS Lambda deployment script for ArXiv Collector
FUNCTION_NAME=arxiv-collector
RUNTIME=python3.13
ARCHITECTURE=x86_64
TIMEOUT=900  # 15 minutes
ROLE_NAME=hackmd-lambda
HANDLER=lambda_function.lambda_handler
LAYER_NAME=collector-layer

echo "========================================"
echo "🚀 Deploying Collector Lambda Function"
echo "========================================"

# Get the role ARN
ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query "Role.Arn" --output text)
echo "📁 Using role: $ROLE_ARN"

# Check if layer exists, if not, create it
if ! aws lambda list-layer-versions --layer-name $LAYER_NAME --query 'LayerVersions[0].LayerVersionArn' --output text >/dev/null 2>&1; then
    echo "🔧 Layer '$LAYER_NAME' doesn't exist. Creating layer..."

    # Create a temporary directory for layer contents
    TEMP_DIR=$(mktemp -d)
    mkdir -p $TEMP_DIR/python

    # Install dependencies with size optimization options
    pip install \
        --platform manylinux2014_x86_64 \
        --python-version 3.13 \
        --only-binary=:all: \
        --no-compile \
        --no-cache-dir \
        --target $TEMP_DIR/python \
        -r src/collect/requirements.txt
    if [ $? -ne 0 ]; then
        echo "❌ Failed to install dependencies"
        exit 1
    fi

    # Create layer zip
    LAYER_ZIP="$LAYER_NAME.zip"
    (cd $TEMP_DIR && zip -r ../$LAYER_ZIP .)
    mv $TEMP_DIR/../$LAYER_ZIP .

    # Create the layer
    LAYER=$(aws lambda publish-layer-version \
        --layer-name $LAYER_NAME \
        --compatible-runtimes $RUNTIME \
        --zip-file fileb://$LAYER_ZIP \
        --query 'LayerVersionArn' \
        --output text)

    echo "✅ Layer created: $LAYER"

    # Clean up
    rm -rf $TEMP_DIR $LAYER_ZIP
else
    # Get the latest layer ARN dynamically
    echo "🔍 Getting latest layer ARN..."
    LAYER=$(aws lambda list-layer-versions --layer-name $LAYER_NAME --query 'LayerVersions[0].LayerVersionArn' --output text)

    if [ -z "$LAYER" ] || [ "$LAYER" = "None" ]; then
        echo "⚠️ Layer '$LAYER_NAME' not found or invalid"
        echo "🔄 Running deploy_layer.sh to create the layer first..."

        # Run the deploy_layer.sh script
        ./src/collect/deploy_layer.sh
        if [ $? -ne 0 ]; then
            echo "❌ Failed to create layer using deploy_layer.sh"
            exit 1
        fi

        # Get the layer ARN again after creation
        LAYER=$(aws lambda list-layer-versions --layer-name $LAYER_NAME --query 'LayerVersions[0].LayerVersionArn' --output text)
        if [ -z "$LAYER" ] || [ "$LAYER" = "None" ]; then
            echo "❌ Error: Still could not retrieve layer ARN after creation attempt"
            exit 1
        fi
    fi

    echo "📁 Using layer: $LAYER"
fi

echo "📦 Zipping function..."
zip -j $FUNCTION_NAME.zip src/collect/*.py

echo "📤 Deploying Lambda function: $FUNCTION_NAME"

# Check if function exists
if aws lambda get-function --function-name $FUNCTION_NAME>/dev/null 2>&1; then
    echo "Updating existing function..."

    # Update function code
    echo "Updating function code..."
    UPDATE_CODE_OUTPUT=$(aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://$FUNCTION_NAME.zip 2>&1)
    if [ $? -eq 0 ]; then
        echo "✅ Function code updated successfully"
    else
        echo "❌ Error: Failed to update function code"
        echo "Error details: $UPDATE_CODE_OUTPUT"
        exit 1
    fi

    # Update function configuration
    sleep 5
    echo "Updating function configuration..."
    UPDATE_CONFIG_OUTPUT=$(aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --layers $LAYER \
        --handler $HANDLER \
        --timeout $TIMEOUT 2>&1)
    if [ $? -eq 0 ]; then
        echo "✅ Function configuration updated successfully"
    else
        echo "❌ Error: Failed to update function configuration"
        echo "Error details: $UPDATE_CONFIG_OUTPUT"
        exit 1
    fi
else
    echo "Creating new function..."

    CREATE_OUTPUT=$(aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --architectures $ARCHITECTURE \
        --runtime $RUNTIME \
        --layers $LAYER \
        --handler $HANDLER \
        --timeout $TIMEOUT \
        --zip-file fileb://$FUNCTION_NAME.zip \
        --role $ROLE_ARN 2>&1)
    if [ $? -eq 0 ]; then
        echo "✅ Function created successfully"
    else
        echo "❌ Error: Failed to create function"
        echo "Error details: $CREATE_OUTPUT"
        exit 1
    fi
fi

# Clean up zip file
rm -f $FUNCTION_NAME.zip

echo "🎉 Lambda deployment completed successfully!"
echo "Function name: $FUNCTION_NAME"
echo "Runtime: $RUNTIME"
echo "Timeout: ${TIMEOUT}s"
echo "Layer: $LAYER"
