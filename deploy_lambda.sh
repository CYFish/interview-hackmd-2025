#!/bin/bash

# AWS Lambda deployment script
FUNCTION_NAME="hackmd-processor"
RUNTIME="python3.13"
TIMEOUT=300  # 5 minutes
ROLE="arn:aws:iam::589535354706:role/hackmd-lambda"
HANDLER="processor.main"
LAYER="arn:aws:lambda:ap-northeast-1:589535354706:layer:process-layer:1"

echo "Zipping function..."
zip -j $FUNCTION_NAME.zip src/process/*

echo "Deploying Lambda function: $FUNCTION_NAME"

# Check if function exists
if aws lambda get-function --function-name $FUNCTION_NAME>/dev/null 2>&1; then
    echo "Updating existing function..."
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://$FUNCTION_NAME.zip

    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --layers $LAYER \
        --timeout $TIMEOUT
else
    echo "Creating new function..."
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime $RUNTIME \
        --layers $LAYER \
        --handler $HANDLER \
        --timeout $TIMEOUT \
        --zip-file fileb://$FUNCTION_NAME.zip \
        --role $ROLE \
        --handler $HANDLER
fi

echo "Deployment completed!"
