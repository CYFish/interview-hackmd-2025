#!/bin/bash

# AWS EventBridge deployment script for ArXiv Processor
FUNCTION_NAME=arxiv-processor
REGION=ap-northeast-1
RULE_NAME="daily-arxiv-processing"
SCHEDULE="cron(30 1 * * ? *)"  # Run daily at 01:30 UTC

echo "========================================"
echo "🚀 Deploying EventBridge Rule for Processor"
echo "========================================"

# Get the Lambda function ARN
FUNCTION_ARN=$(aws lambda get-function --function-name $FUNCTION_NAME --query 'Configuration.FunctionArn' --output text)
if [ -z "$FUNCTION_ARN" ]; then
    echo "❌ Error: Lambda function '$FUNCTION_NAME' not found"
    echo "Please deploy the Lambda function first using deploy_lambda.sh"
    exit 1
fi

echo "📁 Using Lambda function: $FUNCTION_ARN"

# Create or update the EventBridge rule
echo "📝 Creating/updating EventBridge rule..."
RULE_ARN=$(aws events put-rule \
    --name "$RULE_NAME" \
    --schedule-expression "$SCHEDULE" \
    --description "Triggers daily arXiv data processing" \
    --region $REGION \
    --query 'RuleArn' \
    --output text)

if [ $? -ne 0 ]; then
    echo "❌ Failed to create/update EventBridge rule"
    exit 1
fi

echo "✅ EventBridge rule created/updated: $RULE_ARN"

# Create the input JSON for the Lambda target
# We need to escape quotes properly for the AWS CLI
INPUT_JSON="{\\\"s3_bucket\\\": \\\"hackmd-project-2025\\\", \\\"table_name\\\": \\\"arxiv-papers\\\"}"

# Add the Lambda function as a target for the rule
echo "🎯 Setting Lambda function as target..."
TARGET_RESULT=$(aws events put-targets \
    --rule "$RULE_NAME" \
    --targets "[{\"Id\": \"1\", \"Arn\": \"$FUNCTION_ARN\", \"Input\": \"$INPUT_JSON\"}]" \
    --region $REGION)

if [ $? -ne 0 ]; then
    echo "❌ Failed to set Lambda function as target"
    echo "$TARGET_RESULT"
    exit 1
fi

echo "✅ Lambda function set as target for EventBridge rule"

# Add permission for EventBridge to invoke the Lambda function
echo "🔑 Adding permission for EventBridge to invoke Lambda..."

# Check if permission already exists (to avoid duplicate permission errors)
STATEMENT_ID="$RULE_NAME"
PERMISSION_CHECK=$(aws lambda get-policy --function-name $FUNCTION_NAME --query "Policy" --output text 2>/dev/null || echo "")

if [[ "$PERMISSION_CHECK" == *"$STATEMENT_ID"* ]]; then
    echo "⚠️ Permission already exists, skipping permission creation"
else
    PERMISSION_RESULT=$(aws lambda add-permission \
        --function-name $FUNCTION_NAME \
        --statement-id "$STATEMENT_ID" \
        --action "lambda:InvokeFunction" \
        --principal "events.amazonaws.com" \
        --source-arn "$RULE_ARN" \
        --region $REGION)

    if [ $? -ne 0 ]; then
        echo "❌ Failed to add permission"
        echo "$PERMISSION_RESULT"
        exit 1
    fi

    echo "✅ Permission added for EventBridge to invoke Lambda"
fi

echo "🎉 EventBridge deployment completed successfully!"
echo "Rule name: $RULE_NAME"
echo "Schedule: $SCHEDULE (daily at 01:30 UTC)"
echo "Target Lambda: $FUNCTION_NAME"
