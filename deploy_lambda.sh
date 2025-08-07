#!/bin/bash

# AWS Lambda deployment script
FUNCTION_NAME=hackmd-processor
RUNTIME=python3.13
ARCHITECTURE=x86_64
TIMEOUT=300  # 5 minutes
ROLE=arn:aws:iam::589535354706:role/hackmd-lambda
HANDLER=lambda_function.lambda_handler
LAYER_NAME=process-layer

echo "========================================"
echo "🚀 Deploying Lambda Function"
echo "========================================"

# Get the latest layer ARN dynamically
echo "🔍 Getting latest layer ARN..."
LAYER=$(aws lambda list-layer-versions --layer-name $LAYER_NAME --query 'LayerVersions[0].LayerVersionArn' --output text)
if [ -z "$LAYER" ] || [ "$LAYER" = "None" ]; then
    echo "❌ Error: Could not retrieve layer ARN for '$LAYER_NAME'"
    echo "Please ensure the layer exists by running: ./deploy_layer.sh"
    exit 1
fi
echo "📁 Using layer: $LAYER"

echo "📦 Zipping function..."
zip -j $FUNCTION_NAME.zip src/process/*

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
        --role $ROLE 2>&1)
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
