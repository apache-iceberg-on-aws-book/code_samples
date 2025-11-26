# AWS Glue Schema Registry Examples

This project demonstrates how to produce and consume messages using **AWS Glue Schema Registry** with Apache Kafka. 

It showcases data serialization/deserialization across multiple data formats including Avro, JSON, and Protocol Buffers.

## Overview

AWS Glue Schema Registry provides a centralized repository for managing and evolving schemas for streaming data. This project includes complete examples of:

- **Avro** producers and consumers with schema registry integration
- **JSON** producers and consumers with schema validation
- **Protocol Buffers** producers and consumers with type safety
- Local Kafka setup using Docker Compose

## Project Structure

```
├── src/main/
│   ├── avro/
│   │   └── Employee.avsc              # Avro schema definition
│   ├── protobuf/
│   │   └── Employee.proto             # Protocol Buffers schema
│   └── java/com/aws/emr/
│       ├── avro/kafka/
│       │   ├── producer/              # Avro Kafka producers
│       │   └── consumer/              # Avro Kafka consumers
│       ├── json/kafka/
│       │   ├── producer/              # JSON Kafka producers
│       │   └── consumer/              # JSON Kafka consumers
│       └── proto/kafka/
│           ├── producer/              # Protocol Buffers producers
│           └── consumer/              # Protocol Buffers consumers
├── docker-compose.yaml                # Local Kafka setup
└── pom.xml                           # Maven dependencies
```

## Prerequisites

- Java 17+
- Maven 3.6+
- Docker and Docker Compose (for local Kafka)
- AWS CLI configured with appropriate permissions
- AWS Glue Schema Registry access
- For MSK: IAM role/user with permissions for `kafka-cluster:*` actions

## AWS Setup

### 1. Create Schema Registry

```bash
aws glue create-registry --registry-name employee-schema-registry --region your-region
```

### 2. Register Schemas

**Avro Schema:**
```bash
aws glue create-schema \
  --registry-id RegistryName=employee-schema-registry \
  --schema-name Employee.avsc \
  --data-format AVRO \
  --schema-definition file://src/main/avro/Employee.avsc \
  --region your-region
```

**JSON Schema:**
```bash
aws glue create-schema \
  --registry-id RegistryName=employee-schema-registry \
  --schema-name Employee.json \
  --data-format JSON \
  --schema-definition '{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"employeeId":{"type":"integer"},"name":{"type":"string"}}}' \
  --region your-region
```

**Protocol Buffers Schema:**
```bash
aws glue create-schema \
  --registry-id RegistryName=employee-schema-registry \
  --schema-name Employee.proto \
  --data-format PROTOBUF \
  --schema-definition file://src/main/protobuf/Employee.proto \
  --region eu-west-1
```

## Quick Start

### 1. Start Local Kafka or setup MSK cluster

**Local Kafka:**
```bash
docker-compose up -d
```

**MSK Cluster:**
Ensure your MSK cluster is configured with IAM authentication enabled.

### 2. Build the Project

```bash
mvn clean compile
```


### Command Line Arguments

All producers and consumers accept two optional arguments:
1. **Bootstrap Servers**: Kafka broker addresses (default: `localhost:9092`)
2. **MSK IAM Flag**: Enable MSK IAM authentication (default: `false`)

Examples:
```bash
# Local Kafka
java -cp target/classes com.aws.emr.avro.kafka.producer.AvroProducerSchemaRegistry

# Custom bootstrap servers (no IAM)
java -cp target/classes com.aws.emr.avro.kafka.producer.AvroProducerSchemaRegistry "broker:9092"

# MSK with IAM authentication
java -cp target/classes com.aws.emr.avro.kafka.producer.AvroProducerSchemaRegistry "broker:9098" "true"
```

## Schema Formats

### Avro Schema (Employee.avsc)
```json
{
  "namespace": "gsr.avro.post",
  "type": "record",
  "name": "Employee",
  "fields": [
    {"name": "employee_id", "type": "long"},
    {"name": "age", "type": "int"},
    {"name": "start_date", "type": "long"},
    {"name": "team", "type": "string"},
    {"name": "role", "type": "string"},
    {"name": "address", "type": "string"},
    {"name": "name", "type": "string"}
  ]
}
```

### Protocol Buffers Schema (Employee.proto)
```protobuf
syntax = "proto3";
package gsr.proto.post;

message Employee {
  int32 id = 1;
  string name = 2;
  string address = 3;
  google.protobuf.Int32Value employee_age = 4;
  google.protobuf.Timestamp start_date = 5;
  Team team = 6;
  Role role = 7;
}

message Team {
  string name = 1;
  string location = 2;
}

enum Role {
  MANAGER = 0;
  DEVELOPER = 1;
  ARCHITECT = 2;
}
```

## Key Features

### Schema Registry Integration
- Schema evolution support with compatibility checks
- Centralized schema management across applications

### Serialization Formats
- **Avro**: Compact binary format with schema evolution
- **JSON**: Human-readable with schema validation
- **Protocol Buffers**: Type-safe with backward/forward compatibility

### Authentication Support
- **Local Kafka**: No authentication (default)
- **MSK with IAM**: AWS IAM authentication using `aws-msk-iam-auth` library

### Producer Features
- Asynchronous message publishing
- Custom callback handling for delivery confirmation
- Configurable batch processing
- Error handling and retry logic
- MSK IAM authentication support

### Consumer Features
- Automatic schema deserialization
- Consumer group management
- Offset management with earliest/latest positioning
- Graceful error handling
- MSK IAM authentication support


## Topics Created

The examples create the following Kafka topics:
- `avro-demo-topic`: For Avro message examples
- `json-demo-topic`: For JSON message examples  
- `protobuf-demo-topic`: For Protocol Buffers examples


## IAM Permissions for MSK

When using MSK with IAM authentication, ensure your IAM role/user has the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:AlterCluster",
        "kafka-cluster:DescribeCluster"
      ],
      "Resource": "arn:aws:kafka:region:account-id:cluster/cluster-name/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:*Topic*",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData"
      ],
      "Resource": "arn:aws:kafka:region:account-id:topic/cluster-name/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeGroup"
      ],
      "Resource": "arn:aws:kafka:region:account-id:group/cluster-name/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetSchemaVersion",
        "glue:GetSchemaByDefinition",
        "glue:RegisterSchemaVersion"
      ],
      "Resource": "*"
    }
  ]
}
```

## Cleanup

```bash
# Stop Kafka
docker-compose down

# Remove volumes (optional)
docker-compose down -v
```
