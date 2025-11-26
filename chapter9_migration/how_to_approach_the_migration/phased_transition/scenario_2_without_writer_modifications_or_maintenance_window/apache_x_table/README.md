# Automating Apache XTable Sync with Amazon EventBridge and ECS

This section provides instructions to perform a phased transition from Hudi to Iceberg, using in-place migration leveraging the Apache xTable sync tool.

The approach performs the following:
1. In-place migrate a Hudi table to Iceberg
2. Keep Iceberg table in sync with the Hudi table over time 

## Prerequisites

- AWS CLI configured with appropriate permissions
- Docker installed and running
- A Hudi table pre-created that you want to migrate to Iceberg (initial migration) and continuous sync

## 1. Prepare Apache XTable Utilities 
Compile `xtable-utilities` following the instructions in the Apache XTable GitHub repository (https://github.com/apache/incubator-xtable) and  copy `xtable-utilities_2.12-0.3.0-incubating-bundled.jar` in the current working directory

## 2. Configure Apache XTables Configuration files

Update the `datasetConfig.yaml` file with your specific settings:
- `tableBasePath`: the source Hudi table location
- `tableName`: the target Iceberg table name
- `namespace`: the target Database where to create the Iceberg table
- `partitionSpec`: the current partitioning of the source table

Update the  `glueDataCatalog.yaml` file with your specific settings:
- `warehouse`: the Iceberg warehouse location under which databases and tables are created

## 3. Deploy the infrastructure

Deploy the `infrastructure.yaml` CloudFormation stack and wait:

```
#Change with your AWS region
REGION="eu-west-1"
INFRA_STACK_NAME="xtable-sync-infra"
TASK_STACK_NAME="xtable-sync-task"

echo "Deploying infrastructure stack..."
aws cloudformation create-stack \
    --stack-name $INFRA_STACK_NAME \
    --template-body file://infrastructure.yaml \
    --capabilities CAPABILITY_NAMED_IAM \
    --region $REGION

echo "Waiting for infrastructure stack to complete..."
aws cloudformation wait stack-create-complete \
    --stack-name $INFRA_STACK_NAME \
    --region $REGION
```

## 4. Build and Push Docker Image containing Apache XTables JAR and configuration files

Build docker image and push to ECS (Note: docker should be running in your current machine):

```
echo "Getting ECR repository URI..."
ECR_REPO=$(aws cloudformation describe-stacks \
    --stack-name $INFRA_STACK_NAME \
    --query 'Stacks[0].Outputs[?OutputKey==`EcrRepositoryUri`].OutputValue' \
    --output text \
    --region $REGION)

echo "Building and pushing Docker image..."
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_REPO
docker build -t xtable-sync .
docker tag xtable-sync:latest $ECR_REPO:latest
docker push $ECR_REPO:latest
```

## 5. Deploy ECS Task Configuration

Deploy the `task.yaml` CloudFormation stack and wait:

```
echo "Deploying task stack..."
aws cloudformation create-stack \
    --stack-name $TASK_STACK_NAME \
    --template-body file://task.yaml \
    --parameters \
        ParameterKey=InfraStackName,ParameterValue=$INFRA_STACK_NAME \
        ParameterKey=ScheduleExpression,ParameterValue="rate(1 hour)" \
        ParameterKey=ImageTag,ParameterValue=latest \
    --capabilities CAPABILITY_NAMED_IAM \
    --region $REGION
```

## Verification

- Check ECS cluster status in AWS Console
- Monitor task execution in ECS console and task logs in CloudWatch Logs console
- Verify Iceberg table creation in Glue Data Catalog
- Confirm data synchronization between Hudi and Iceberg tables


## Notes
- The sync job runs every hour by default (configurable via `ScheduleExpression`)
- Adjust resource allocations in task definition if needed
- Consider implementing monitoring and alerting for failed tasks



At this point you have an ECS cluster and an ECS task that is triggered every 1 hour that run the Apache XTable sync utility to keep your Hudi table in sync.

If you keep writing to your Hudi table you will see after every sync the Iceberg table aligning.

Note this will work for a Hudi COW table. If using a MOR table the resulting Iceberg table will match the Hudi Read Optimized view table (eg., `<hudi_table_name>_ro`).