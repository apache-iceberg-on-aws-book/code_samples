## Prerequisites and Setup

Before diving into the AWS service integrations, you'll need to set up the foundational AWS resources that all examples in this chapter will use. This section provides step-by-step instructions to prepare your AWS environment.

### AWS Account Setup

#### 1. AWS CLI Configuration

First, ensure your AWS CLI is properly configured with credentials and default region:

```bash
# Install AWS CLI if not already installed
# macOS: brew install awscli
# Linux: pip install awscli

# Configure AWS CLI with your credentials
aws configure
# Enter your Access Key ID, Secret Access Key, region (e.g., us-east-1), and output format (json)

# Verify configuration
aws sts get-caller-identity
```

#### 2. Create S3 Bucket for Examples

Create a dedicated S3 bucket for storing Iceberg table data throughout this chapter:

```bash
# Get account ID and region from AWS CLI
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=$(aws configure get region)
export BUCKET_NAME="iceberg-book-examples-${AWS_ACCOUNT_ID}-${AWS_REGION}"

echo "Account ID: $AWS_ACCOUNT_ID"
echo "Region: $AWS_REGION"
echo "Bucket name: $BUCKET_NAME"

# Create S3 bucket
aws s3api create-bucket --bucket $BUCKET_NAME --region $AWS_REGION

# Verify bucket creation
aws s3 ls | grep $BUCKET_NAME
```

#### 3. Create IAM Roles

Create the necessary IAM roles for AWS services to access your Iceberg data:

```bash
# Create trust policy for Glue
cat > glue-trust-policy.json << EOF
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

# Create Glue service role
aws iam create-role \
    --role-name IcebergBookGlueRole \
    --assume-role-policy-document file://glue-trust-policy.json

# Attach managed policies
aws iam attach-role-policy \
    --role-name IcebergBookGlueRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Create custom policy for S3 access
cat > glue-s3-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::$BUCKET_NAME",
        "arn:aws:s3:::$BUCKET_NAME/*"
      ]
    }
  ]
}
EOF

aws iam create-policy \
    --policy-name IcebergBookS3Policy \
    --policy-document file://glue-s3-policy.json

aws iam attach-role-policy \
    --role-name IcebergBookGlueRole \
    --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/IcebergBookS3Policy
```

#### 4. Create Glue Database

Create a Glue database to organize your Iceberg tables:

```bash
# Create Glue database for examples
aws glue create-database \
    --database-input Name=iceberg_examples,Description="Database for Iceberg book examples"

# Verify database creation
aws glue get-database --name iceberg_examples
```

#### 5. Environment Variables

Set up environment variables for consistent usage throughout the chapter:

```bash
# Get dynamic values
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=$(aws configure get region)

# Set derived values
export ICEBERG_BUCKET_NAME="iceberg-book-examples-${AWS_ACCOUNT_ID}-${AWS_REGION}"
export ICEBERG_AWS_REGION="$AWS_REGION"
export ICEBERG_GLUE_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/IcebergBookGlueRole"
export ICEBERG_DATABASE_NAME="iceberg_examples"

# Display values for verification
echo "Account ID: $AWS_ACCOUNT_ID"
echo "Region: $AWS_REGION"
echo "Bucket: $ICEBERG_BUCKET_NAME"
echo "Glue Role ARN: $ICEBERG_GLUE_ROLE_ARN"
echo "Database: $ICEBERG_DATABASE_NAME"
```

Add these environment variables to your shell profile (`.bashrc`, `.zshrc`, etc.) for persistence across sessions.

### Setup Verification

Before proceeding with the service-specific sections, verify your setup is complete:

```bash
# Check AWS CLI access
aws sts get-caller-identity

# Verify S3 bucket exists
aws s3 ls s3://$ICEBERG_BUCKET_NAME

# Verify IAM role exists
aws iam get-role --role-name IcebergBookGlueRole

# Verify Glue database exists
aws glue get-database --name iceberg_examples

# Test S3 write permissions
echo "test" | aws s3 cp - s3://$ICEBERG_BUCKET_NAME/test.txt
aws s3 rm s3://$ICEBERG_BUCKET_NAME/test.txt
```

If all commands execute successfully, your environment is ready for the examples in this chapter.
