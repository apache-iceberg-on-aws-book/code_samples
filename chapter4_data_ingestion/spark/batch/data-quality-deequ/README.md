# Deequ Data Quality Example (Java)

Using DQDL with Open Source Spark demonstrating data quality validation using Amazon Deequ's DQDL (Data Quality Definition Language).

## Overview

This example validates customer data using predefined quality rules including completeness, uniqueness, and custom business logic checks.

## Files

- `DeequDQDLExample.java` - Main application with sample data and validation logic
- `dqdl_rules.txt` - DQDL syntax rules for validation

## Running on AWS EMR Serverless

Note that you will need to modify the code to read the ruleset on the Spark driver on an S3 bucket. You will need to download the corresponging S3 libraries. 

For example: 
```xml
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>s3</artifactId>
  <version>your-version</version>
</dependency>
```

Then you can use the official [docs](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/java_s3_code_examples.html) as guide. 


1. Package your application as a JAR
2. Upload to S3 along with the rules files
3. Submit job to EMR Serverless:

```bash
aws emr-serverless start-job-run \
  --application-id <your-app-id> \
  --execution-role-arn <your-role-arn> \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://your-bucket/deequ-example.jar",
      "entryPointArguments": [],
      "sparkSubmitParameters": "--class com.aws.deequ.DeequDQDLExample"
    }
  }'
```


## Dependencies

- Apache Spark 3.5
- Amazon Deequ library
- Java 17
