# CloudWatch Alerting Configuration - Complete Executable Guide

This guide provides **complete step-by-step instructions** that you can execute from start to finish, including creating sample metrics and testing the complete alerting system.

## Prerequisites Setup

### Step 1: Verify AWS CLI and Permissions

```bash
# Verify AWS CLI is installed and configured
aws --version

# Verify AWS credentials and permissions
aws sts get-caller-identity

# Test CloudWatch permissions
aws cloudwatch list-metrics --max-items 1

# Test SNS permissions
aws sns list-topics --max-items 1
```

### Step 2: Set Up Working Directory

```bash
# Create a working directory for this exercise
mkdir iceberg-alerting-demo
cd iceberg-alerting-demo

# Create a log file to track our progress
echo "$(date): Starting Iceberg alerting configuration" > setup.log
```

## Part 1: Create Sample Metrics for Testing

### Step 3: Create Sample CloudWatch Metrics

Since we need metrics to create meaningful alarms, let's create sample Iceberg monitoring metrics:

```bash
# Create a script to publish sample Iceberg metrics
cat > publish_sample_metrics.py << 'EOF'
#!/usr/bin/env python3
import boto3
import random
import time
from datetime import datetime

def publish_sample_metrics():
    """Publish sample Iceberg metrics to CloudWatch for testing"""
    cloudwatch = boto3.client('cloudwatch')
    
    # Sample metrics that represent realistic Iceberg monitoring scenarios
    metrics_data = [
        # Table health metrics
        {
            'namespace': 'Iceberg/TableHealth',
            'metrics': [
                {'name': 'SmallFilesPercentage', 'value': 35.5, 'unit': 'Percent', 'table': 'transactions'},
                {'name': 'SmallFilesPercentage', 'value': 15.2, 'unit': 'Percent', 'table': 'customers'},
                {'name': 'SmallFilesPercentage', 'value': 65.8, 'unit': 'Percent', 'table': 'products'},
                {'name': 'TotalSnapshots', 'value': 45, 'unit': 'Count', 'table': 'transactions'},
                {'name': 'TotalSnapshots', 'value': 12, 'unit': 'Count', 'table': 'customers'},
                {'name': 'TotalSnapshots', 'value': 120, 'unit': 'Count', 'table': 'products'},
                {'name': 'OldestSnapshotAgeDays', 'value': 15, 'unit': 'Count', 'table': 'transactions'},
                {'name': 'OldestSnapshotAgeDays', 'value': 5, 'unit': 'Count', 'table': 'customers'},
                {'name': 'OldestSnapshotAgeDays', 'value': 35, 'unit': 'Count', 'table': 'products'},
            ]
        },
        # Query performance metrics
        {
            'namespace': 'Iceberg/QueryPerformance',
            'metrics': [
                {'name': 'QueryExecutionTime', 'value': 25000, 'unit': 'Milliseconds', 'table': 'transactions'},
                {'name': 'QueryExecutionTime', 'value': 8000, 'unit': 'Milliseconds', 'table': 'customers'},
                {'name': 'QueryExecutionTime', 'value': 45000, 'unit': 'Milliseconds', 'table': 'products'},
                {'name': 'DataScannedBytes', 'value': 500000000000, 'unit': 'Bytes', 'table': 'transactions'},
                {'name': 'DataScannedBytes', 'value': 100000000000, 'unit': 'Bytes', 'table': 'customers'},
                {'name': 'DataScannedBytes', 'value': 1200000000000, 'unit': 'Bytes', 'table': 'products'},
            ]
        },
        # EMR maintenance metrics
        {
            'namespace': 'Iceberg/EMR',
            'metrics': [
                {'name': 'StepDurationSeconds', 'value': 3600, 'unit': 'Seconds', 'step': 'compaction'},
                {'name': 'StepDurationSeconds', 'value': 1800, 'unit': 'Seconds', 'step': 'expire_snapshots'},
                {'name': 'StepFailure', 'value': 0, 'unit': 'Count', 'step': 'compaction'},
                {'name': 'StepFailure', 'value': 1, 'unit': 'Count', 'step': 'expire_snapshots'},
            ]
        }
    ]
    
    published_count = 0
    
    for namespace_data in metrics_data:
        namespace = namespace_data['namespace']
        
        for metric in namespace_data['metrics']:
            dimensions = []
            
            # Add appropriate dimensions based on the metric
            if 'table' in metric:
                dimensions.append({'Name': 'Table', 'Value': metric['table']})
            if 'step' in metric:
                dimensions.append({'Name': 'StepName', 'Value': metric['step']})
            
            # Add some randomness to make metrics more realistic
            base_value = metric['value']
            random_factor = random.uniform(0.8, 1.2)  # ±20% variation
            actual_value = base_value * random_factor
            
            try:
                response = cloudwatch.put_metric_data(
                    Namespace=namespace,
                    MetricData=[
                        {
                            'MetricName': metric['name'],
                            'Value': actual_value,
                            'Unit': metric['unit'],
                            'Dimensions': dimensions
                        }
                    ]
                )
                
                print(f"✅ Published {metric['name']} = {actual_value:.2f} {metric['unit']} to {namespace}")
                published_count += 1
                
                # Small delay to avoid throttling
                time.sleep(0.1)
                
            except Exception as e:
                print(f"❌ Failed to publish {metric['name']}: {e}")
    
    return published_count

if __name__ == "__main__":
    try:
        count = publish_sample_metrics()
        print(f"\n✅ Successfully published {count} sample metrics")
        print("These metrics will be used to test our alerting configuration")
    except Exception as e:
        print(f"❌ Error: {e}")
EOF

# Make the script executable
chmod +x publish_sample_metrics.py

# Run the script to create sample metrics
python3 publish_sample_metrics.py
```

### Step 4: Verify Sample Metrics Were Created

```bash
# Wait for metrics to appear in CloudWatch
echo "Waiting 30 seconds for metrics to appear in CloudWatch..."
sleep 30

# List metrics in each namespace
echo "=== Iceberg Table Health Metrics ==="
aws cloudwatch list-metrics --namespace "Iceberg/TableHealth"

echo -e "\n=== Iceberg Query Performance Metrics ==="
aws cloudwatch list-metrics --namespace "Iceberg/QueryPerformance"

echo -e "\n=== Iceberg EMR Metrics ==="
aws cloudwatch list-metrics --namespace "Iceberg/EMR"
```

## Part 2: Configure SNS Topics and Subscriptions

### Step 5: Create SNS Topics for Different Alert Severities

```bash
# Create SNS topics for different alert levels
echo "Creating SNS topics for alerting..."

# Create critical alerts topic
aws sns create-topic --name iceberg-critical-alerts
echo "arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:iceberg-critical-alerts" > critical_topic_arn.txt

# Create warning alerts topic
aws sns create-topic --name iceberg-warning-alerts
echo "arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:iceberg-warning-alerts" > warning_topic_arn.txt

# Create informational alerts topic
aws sns create-topic --name iceberg-info-alerts
echo "arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:iceberg-info-alerts" > info_topic_arn.txt

# Note: Replace YOUR_ACCOUNT_ID with your actual AWS account ID from the create-topic output

# Verify topics were created
echo -e "\n=== Created SNS Topics ==="
aws sns list-topics | grep iceberg
```

### Step 6: Subscribe to SNS Topics

```bash
# Subscribe email addresses to topics
echo "Setting up email subscriptions..."

# Subscribe to critical alerts (replace with your email)
aws sns subscribe \
    --topic-arn $(cat critical_topic_arn.txt) \
    --protocol email \
    --notification-endpoint your-email@company.com

# Subscribe to warning alerts (replace with your email)
aws sns subscribe \
    --topic-arn $(cat warning_topic_arn.txt) \
    --protocol email \
    --notification-endpoint your-email@company.com

echo "✅ Email subscriptions created. Please check your email and confirm subscriptions."
```

### Step 7: Test SNS Topics

```bash
# Test each SNS topic to ensure notifications work
echo "Testing SNS topics..."

# Test critical alerts topic
aws sns publish \
    --topic-arn $(cat critical_topic_arn.txt) \
    --subject "TEST: Iceberg Critical Alert System" \
    --message "This is a test message to verify the critical alerting system is working correctly. If you receive this, the setup is successful."

# Test warning alerts topic
aws sns publish \
    --topic-arn $(cat warning_topic_arn.txt) \
    --subject "TEST: Iceberg Warning Alert System" \
    --message "This is a test message to verify the warning alerting system is working correctly. If you receive this, the setup is successful."

echo "✅ Test messages sent. Check your email for test notifications."
```

## Part 3: Create Table Health Alarms

### Step 8: Create Small Files Percentage Alarms

```bash
# Create alarms for small files percentage
echo "Creating small files percentage alarms..."

# Critical alarm for transactions table (>50% small files)
aws cloudwatch put-metric-alarm \
    --alarm-name "Iceberg-SmallFiles-Critical-Transactions" \
    --alarm-description "Critical: Small files percentage exceeds 50% for transactions table" \
    --metric-name SmallFilesPercentage \
    --namespace Iceberg/TableHealth \
    --statistic Average \
    --period 3600 \
    --threshold 50 \
    --comparison-operator GreaterThanThreshold \
    --dimensions Name=Table,Value=transactions \
    --evaluation-periods 2 \
    --datapoints-to-alarm 2 \
    --treat-missing-data notBreaching \
    --alarm-actions $(cat critical_topic_arn.txt) \
    --ok-actions $(cat critical_topic_arn.txt) \
    --tags Key=Environment,Value=demo Key=Service,Value=iceberg Key=Severity,Value=critical

# Warning alarm for transactions table (>25% small files)
aws cloudwatch put-metric-alarm \
    --alarm-name "Iceberg-SmallFiles-Warning-Transactions" \
    --alarm-description "Warning: Small files percentage exceeds 25% for transactions table" \
    --metric-name SmallFilesPercentage \
    --namespace Iceberg/TableHealth \
    --statistic Average \
    --period 3600 \
    --threshold 25 \
    --comparison-operator GreaterThanThreshold \
    --dimensions Name=Table,Value=transactions \
    --evaluation-periods 3 \
    --datapoints-to-alarm 2 \
    --treat-missing-data notBreaching \
    --alarm-actions $(cat warning_topic_arn.txt) \
    --ok-actions $(cat warning_topic_arn.txt) \
    --tags Key=Environment,Value=demo Key=Service,Value=iceberg Key=Severity,Value=warning

# Critical alarm for products table (>60% small files)
aws cloudwatch put-metric-alarm \
    --alarm-name "Iceberg-SmallFiles-Critical-Products" \
    --alarm-description "Critical: Small files percentage exceeds 60% for products table" \
    --metric-name SmallFilesPercentage \
    --namespace Iceberg/TableHealth \
    --statistic Average \
    --period 3600 \
    --threshold 60 \
    --comparison-operator GreaterThanThreshold \
    --dimensions Name=Table,Value=products \
    --evaluation-periods 2 \
    --datapoints-to-alarm 2 \
    --treat-missing-data notBreaching \
    --alarm-actions $(cat critical_topic_arn.txt) \
    --ok-actions $(cat critical_topic_arn.txt) \
    --tags Key=Environment,Value=demo Key=Service,Value=iceberg Key=Severity,Value=critical

echo "✅ Small files percentage alarms created"
```

### Step 9: Create Snapshot Accumulation Alarms

```bash
# Create alarms for snapshot accumulation
echo "Creating snapshot accumulation alarms..."

# Critical alarm for excessive snapshot accumulation
aws cloudwatch put-metric-alarm \
    --alarm-name "Iceberg-SnapshotAccumulation-Critical" \
    --alarm-description "Critical: Snapshot count exceeds 100 for table" \
    --metric-name TotalSnapshots \
    --namespace Iceberg/TableHealth \
    --statistic Maximum \
    --period 3600 \
    --threshold 100 \
    --comparison-operator GreaterThanThreshold \
    --dimensions Name=Table,Value=products \
    --evaluation-periods 1 \
    --datapoints-to-alarm 1 \
    --treat-missing-data notBreaching \
    --alarm-actions $(cat critical_topic_arn.txt) \
    --tags Key=Environment,Value=demo Key=Service,Value=iceberg Key=Severity,Value=critical

# Warning alarm for snapshot age
aws cloudwatch put-metric-alarm \
    --alarm-name "Iceberg-SnapshotAge-Warning" \
    --alarm-description "Warning: Oldest snapshot exceeds 30 days" \
    --metric-name OldestSnapshotAgeDays \
    --namespace Iceberg/TableHealth \
    --statistic Maximum \
    --period 86400 \
    --threshold 30 \
    --comparison-operator GreaterThanThreshold \
    --dimensions Name=Table,Value=products \
    --evaluation-periods 1 \
    --datapoints-to-alarm 1 \
    --treat-missing-data notBreaching \
    --alarm-actions $(cat warning_topic_arn.txt) \
    --tags Key=Environment,Value=demo Key=Service,Value=iceberg Key=Severity,Value=warning

echo "✅ Snapshot accumulation alarms created"
```

## Part 4: Create Maintenance Operation Alarms

### Step 10: Create EMR Maintenance Alarms

```bash
# Create alarms for EMR maintenance operations
echo "Creating EMR maintenance operation alarms..."

# Alert for maintenance operation failures
aws cloudwatch put-metric-alarm \
    --alarm-name "Iceberg-MaintenanceFailure-Critical" \
    --alarm-description "Critical: Maintenance operation failed" \
    --metric-name StepFailure \
    --namespace Iceberg/EMR \
    --statistic Sum \
    --period 3600 \
    --threshold 1 \
    --comparison-operator GreaterThanOrEqualToThreshold \
    --evaluation-periods 1 \
    --datapoints-to-alarm 1 \
    --treat-missing-data notBreaching \
    --alarm-actions $(cat critical_topic_arn.txt) \
    --tags Key=Environment,Value=demo Key=Service,Value=iceberg Key=Severity,Value=critical

# Alert for maintenance operation duration anomalies
aws cloudwatch put-metric-alarm \
    --alarm-name "Iceberg-MaintenanceDuration-Warning" \
    --alarm-description "Warning: Maintenance operation taking longer than expected" \
    --metric-name StepDurationSeconds \
    --namespace Iceberg/EMR \
    --statistic Average \
    --period 3600 \
    --threshold 7200 \
    --comparison-operator GreaterThanThreshold \
    --dimensions Name=StepName,Value=compaction \
    --evaluation-periods 1 \
    --datapoints-to-alarm 1 \
    --treat-missing-data notBreaching \
    --alarm-actions $(cat warning_topic_arn.txt) \
    --tags Key=Environment,Value=demo Key=Service,Value=iceberg Key=Severity,Value=warning

echo "✅ EMR maintenance operation alarms created"
```

## Part 5: Create Query Performance Alarms

### Step 11: Create Query Performance Alarms

```bash
# Create alarms for query performance monitoring
echo "Creating query performance alarms..."

# Alert for query performance degradation
aws cloudwatch put-metric-alarm \
    --alarm-name "Iceberg-QueryPerformance-Warning" \
    --alarm-description "Warning: Query execution time exceeds SLA threshold" \
    --metric-name QueryExecutionTime \
    --namespace Iceberg/QueryPerformance \
    --statistic Average \
    --period 3600 \
    --threshold 30000 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 2 \
    --datapoints-to-alarm 2 \
    --treat-missing-data notBreaching \
    --alarm-actions $(cat warning_topic_arn.txt) \
    --tags Key=Environment,Value=demo Key=Service,Value=iceberg Key=Severity,Value=warning

# Alert for excessive data scanning (cost efficiency)
aws cloudwatch put-metric-alarm \
    --alarm-name "Iceberg-DataScanning-Cost-Warning" \
    --alarm-description "Warning: Daily data scanned exceeds cost threshold" \
    --metric-name DataScannedBytes \
    --namespace Iceberg/QueryPerformance \
    --statistic Sum \
    --period 86400 \
    --threshold 1099511627776 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 1 \
    --datapoints-to-alarm 1 \
    --treat-missing-data notBreaching \
    --alarm-actions $(cat warning_topic_arn.txt) \
    --tags Key=Environment,Value=demo Key=Service,Value=iceberg Key=Severity,Value=warning

echo "✅ Query performance alarms created"
```

## Part 6: Create Composite Alarms

### Step 12: Create Composite Alarms for Intelligent Alerting

```bash
# Create composite alarms that combine multiple conditions
echo "Creating composite alarms..."

# Composite alarm for overall table health
aws cloudwatch put-composite-alarm \
    --alarm-name "Iceberg-TableHealth-Composite" \
    --alarm-description "Composite: Overall table health degradation" \
    --alarm-rule "(ALARM('Iceberg-SmallFiles-Warning-Transactions') OR ALARM('Iceberg-SnapshotAccumulation-Critical')) AND ALARM('Iceberg-QueryPerformance-Warning')" \
    --actions-enabled \
    --alarm-actions $(cat critical_topic_arn.txt) \
    --ok-actions $(cat critical_topic_arn.txt) \
    --tags Key=Environment,Value=demo Key=Service,Value=iceberg Key=Type,Value=composite

echo "✅ Composite alarms created"
```

## Part 7: Test the Complete Alerting System

### Step 13: Trigger Test Alerts

```bash
# Create script to trigger test alerts by publishing metrics that exceed thresholds
cat > trigger_test_alerts.py << 'EOF'
#!/usr/bin/env python3
import boto3
import time

def trigger_test_alerts():
    """Publish metrics that will trigger our alarms"""
    cloudwatch = boto3.client('cloudwatch')
    
    # Metrics that should trigger alarms
    test_metrics = [
        # This should trigger the small files critical alarm for products
        {
            'namespace': 'Iceberg/TableHealth',
            'name': 'SmallFilesPercentage',
            'value': 75.0,  # Above 60% threshold
            'unit': 'Percent',
            'dimensions': [{'Name': 'Table', 'Value': 'products'}]
        },
        # This should trigger the snapshot accumulation alarm
        {
            'namespace': 'Iceberg/TableHealth',
            'name': 'TotalSnapshots',
            'value': 150.0,  # Above 100 threshold
            'unit': 'Count',
            'dimensions': [{'Name': 'Table', 'Value': 'products'}]
        },
        # This should trigger the query performance warning
        {
            'namespace': 'Iceberg/QueryPerformance',
            'name': 'QueryExecutionTime',
            'value': 45000,  # Above 30000ms threshold
            'unit': 'Milliseconds',
            'dimensions': [{'Name': 'Table', 'Value': 'transactions'}]
        }
    ]
    
    for metric in test_metrics:
        try:
            response = cloudwatch.put_metric_data(
                Namespace=metric['namespace'],
                MetricData=[
                    {
                        'MetricName': metric['name'],
                        'Value': metric['value'],
                        'Unit': metric['unit'],
                        'Dimensions': metric['dimensions']
                    }
                ]
            )
            
            print(f"✅ Published test metric: {metric['name']} = {metric['value']} {metric['unit']}")
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ Failed to publish {metric['name']}: {e}")

if __name__ == "__main__":
    print("Publishing metrics to trigger test alerts...")
    trigger_test_alerts()
    print("\n✅ Test metrics published. Alarms should trigger within 5-10 minutes.")
    print("Check your email for alert notifications.")
EOF

# Make the script executable and run it
chmod +x trigger_test_alerts.py
python3 trigger_test_alerts.py
```

### Step 14: Monitor Alarm States

```bash
# Wait for alarms to process the new metrics
echo "Waiting 5 minutes for alarms to process metrics..."
sleep 300

# Check the state of all our alarms
echo "=== Current Alarm States ==="
aws cloudwatch describe-alarms --alarm-name-prefix "Iceberg-" --query 'MetricAlarms[*].[AlarmName,StateValue,StateReason]' --output table

# Get detailed information about triggered alarms
echo -e "\n=== Detailed Alarm Information ==="
aws cloudwatch describe-alarms --state-value ALARM --alarm-name-prefix "Iceberg-"
```

## Part 8: Create Alarm Management Tools

### Step 15: Create Alarm Management Scripts

```bash
# Create script to manage alarms in bulk
cat > manage_alarms.sh << 'EOF'
#!/bin/bash

case "$1" in
    "disable")
        echo "Disabling all Iceberg alarms..."
        aws cloudwatch describe-alarms \
            --alarm-name-prefix "Iceberg-" \
            --query 'MetricAlarms[*].AlarmName' \
            --output text | xargs -n1 aws cloudwatch disable-alarm-actions --alarm-names
        echo "✅ All Iceberg alarms disabled"
        ;;
    "enable")
        echo "Enabling all Iceberg alarms..."
        aws cloudwatch describe-alarms \
            --alarm-name-prefix "Iceberg-" \
            --query 'MetricAlarms[*].AlarmName' \
            --output text | xargs -n1 aws cloudwatch enable-alarm-actions --alarm-names
        echo "✅ All Iceberg alarms enabled"
        ;;
    "list")
        echo "=== All Iceberg Alarms ==="
        aws cloudwatch describe-alarms \
            --alarm-name-prefix "Iceberg-" \
            --query 'MetricAlarms[*].[AlarmName,StateValue,ActionsEnabled]' \
            --output table
        ;;
    "history")
        echo "=== Recent Alarm History ==="
        aws cloudwatch describe-alarm-history \
            --alarm-name "$2" \
            --max-records 5 \
            --query 'AlarmHistoryItems[*].[Timestamp,HistoryItemType,HistorySummary]' \
            --output table
        ;;
    *)
        echo "Usage: $0 {disable|enable|list|history <alarm-name>}"
        echo "  disable  - Disable all Iceberg alarms"
        echo "  enable   - Enable all Iceberg alarms"
        echo "  list     - List all Iceberg alarms and their states"
        echo "  history  - Show history for a specific alarm"
        ;;
esac
EOF

chmod +x manage_alarms.sh

# Test the alarm management script
echo "Testing alarm management script..."
./manage_alarms.sh list
```

## Part 9: Create Monitoring Dashboard

### Step 16: Create Real-time Alarm Status Dashboard

```bash
# Create a script to show current alarm status
cat > alarm_status_dashboard.py << 'EOF'
#!/usr/bin/env python3
import boto3
import json
from datetime import datetime

def get_alarm_status():
    """Get current status of all Iceberg alarms"""
    cloudwatch = boto3.client('cloudwatch')
    
    try:
        # Get all Iceberg alarms
        response = cloudwatch.describe_alarms(AlarmNamePrefix='Iceberg-')
        alarms = response['MetricAlarms']
        
        # Organize alarms by state
        alarm_states = {
            'ALARM': [],
            'OK': [],
            'INSUFFICIENT_DATA': []
        }
        
        for alarm in alarms:
            state = alarm['StateValue']
            alarm_info = {
                'name': alarm['AlarmName'],
                'description': alarm['AlarmDescription'],
                'state_reason': alarm['StateReason'],
                'state_updated': alarm['StateUpdatedTimestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'actions_enabled': alarm['ActionsEnabled']
            }
            alarm_states[state].append(alarm_info)
        
        return alarm_states
        
    except Exception as e:
        print(f"Error getting alarm status: {e}")
        return None

def display_alarm_dashboard():
    """Display a formatted alarm status dashboard"""
    alarm_states = get_alarm_status()
    
    if not alarm_states:
        return
    
    print("=" * 80)
    print("ICEBERG MONITORING ALARM DASHBOARD")
    print("=" * 80)
    print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Show critical alarms first
    if alarm_states['ALARM']:
        print("🚨 ACTIVE ALARMS (Immediate attention required)")
        print("-" * 50)
        for alarm in alarm_states['ALARM']:
            print(f"❌ {alarm['name']}")
            print(f"   Description: {alarm['description']}")
            print(f"   Reason: {alarm['state_reason']}")
            print(f"   Updated: {alarm['state_updated']}")
            print(f"   Actions Enabled: {alarm['actions_enabled']}")
            print()
    
    # Show OK alarms
    if alarm_states['OK']:
        print("✅ HEALTHY ALARMS")
        print("-" * 50)
        for alarm in alarm_states['OK']:
            print(f"✅ {alarm['name']} - {alarm['state_reason']}")
        print()
    
    # Show insufficient data alarms
    if alarm_states['INSUFFICIENT_DATA']:
        print("⚠️  INSUFFICIENT DATA ALARMS")
        print("-" * 50)
        for alarm in alarm_states['INSUFFICIENT_DATA']:
            print(f"⚠️  {alarm['name']} - {alarm['state_reason']}")
        print()
    
    # Summary
    total_alarms = len(alarm_states['ALARM']) + len(alarm_states['OK']) + len(alarm_states['INSUFFICIENT_DATA'])
    print("=" * 80)
    print(f"SUMMARY: {len(alarm_states['ALARM'])} Active | {len(alarm_states['OK'])} OK | {len(alarm_states['INSUFFICIENT_DATA'])} Insufficient Data | {total_alarms} Total")
    print("=" * 80)

if __name__ == "__main__":
    display_alarm_dashboard()
EOF

chmod +x alarm_status_dashboard.py

# Run the dashboard
echo "=== ALARM STATUS DASHBOARD ==="
python3 alarm_status_dashboard.py
```

## Part 10: Cleanup and Documentation

### Step 17: Create Cleanup Script

```bash
# Create comprehensive cleanup script
cat > cleanup_alerting.sh << 'EOF'
#!/bin/bash

echo "🧹 Cleaning up Iceberg alerting configuration..."

# Read saved topic ARNs
CRITICAL_TOPIC_ARN=$(cat critical_topic_arn.txt 2>/dev/null || echo "")
WARNING_TOPIC_ARN=$(cat warning_topic_arn.txt 2>/dev/null || echo "")
INFO_TOPIC_ARN=$(cat info_topic_arn.txt 2>/dev/null || echo "")

# Delete all Iceberg CloudWatch alarms
echo "Deleting CloudWatch alarms..."
ALARM_NAMES=$(aws cloudwatch describe-alarms --alarm-name-prefix "Iceberg-" --query 'MetricAlarms[*].AlarmName' --output text)
if [ ! -z "$ALARM_NAMES" ]; then
    echo $ALARM_NAMES | xargs aws cloudwatch delete-alarms --alarm-names
    echo "✅ Deleted CloudWatch alarms"
else
    echo "No alarms to delete"
fi

# Delete composite alarms
COMPOSITE_ALARMS=$(aws cloudwatch describe-alarms --alarm-name-prefix "Iceberg-" --alarm-types CompositeAlarm --query 'CompositeAlarms[*].AlarmName' --output text)
if [ ! -z "$COMPOSITE_ALARMS" ]; then
    echo $COMPOSITE_ALARMS | xargs aws cloudwatch delete-alarms --alarm-names
    echo "✅ Deleted composite alarms"
fi

# Delete SNS topics
if [ ! -z "$CRITICAL_TOPIC_ARN" ]; then
    echo "Deleting critical alerts topic: $CRITICAL_TOPIC_ARN"
    aws sns delete-topic --topic-arn "$CRITICAL_TOPIC_ARN" 2>/dev/null || echo "Topic may not exist"
fi

if [ ! -z "$WARNING_TOPIC_ARN" ]; then
    echo "Deleting warning alerts topic: $WARNING_TOPIC_ARN"
    aws sns delete-topic --topic-arn "$WARNING_TOPIC_ARN" 2>/dev/null || echo "Topic may not exist"
fi

if [ ! -z "$INFO_TOPIC_ARN" ]; then
    echo "Deleting info alerts topic: $INFO_TOPIC_ARN"
    aws sns delete-topic --topic-arn "$INFO_TOPIC_ARN" 2>/dev/null || echo "Topic may not exist"
fi

# Remove local files
echo "Removing local files..."
rm -f critical_topic_arn.txt
rm -f warning_topic_arn.txt
rm -f info_topic_arn.txt
rm -f publish_sample_metrics.py
rm -f trigger_test_alerts.py
rm -f manage_alarms.sh
rm -f alarm_status_dashboard.py
rm -f setup.log

echo "✅ Cleanup complete!"
echo "Note: Sample metrics in CloudWatch will expire automatically after 15 months"
EOF

chmod +x cleanup_alerting.sh
echo "✅ Cleanup script created. Run './cleanup_alerting.sh' to remove all resources."
```

## Verification Summary

This guide has created and tested:

1. ✅ **Sample CloudWatch metrics** representing realistic Iceberg monitoring scenarios
2. ✅ **SNS topics and subscriptions** for tiered alerting (critical, warning, informational)
3. ✅ **Comprehensive alarm coverage** for table health, maintenance, and query performance
4. ✅ **Composite alarms** for intelligent alerting based on multiple conditions
5. ✅ **Alert testing system** to verify notifications work correctly
6. ✅ **Alarm management tools** for bulk operations and monitoring
7. ✅ **Real-time dashboard** for alarm status monitoring
8. ✅ **Complete cleanup procedures** for resource management

Every step in this guide has been designed to be executable and includes verification commands to ensure it works correctly.

## Next Steps for Production Use

### Immediate Actions
1. **Replace sample metrics** with real Iceberg monitoring data from your tables
2. **Update email addresses** in SNS subscriptions to your actual team addresses
3. **Adjust alarm thresholds** based on your SLA requirements and baseline metrics
4. **Configure additional notification channels** (Slack, PagerDuty, etc.)

### Customization Required
- **Table names**: Update dimensions to match your actual Iceberg table names
- **Thresholds**: Adjust based on your performance and operational requirements
- **Notification recipients**: Configure appropriate team distribution lists
- **Runbook links**: Add links to your operational procedures in alarm descriptions

### Advanced Integrations
- **Automated remediation**: Set up Lambda functions to automatically trigger maintenance
- **Dashboard integration**: Connect to Grafana, DataDog, or other monitoring platforms
- **Incident management**: Integrate with PagerDuty, Opsgenie, or ServiceNow
- **Cost optimization**: Add budget alerts and cost anomaly detection

## Converting Sample Metrics to Production

To use this with real Iceberg monitoring data, replace the sample metric publishing with actual data collection from:

- **Iceberg metadata tables**: Query snapshot counts, file sizes, and table statistics
- **Query engines**: Collect execution times and data scan metrics from Athena, Spark, or Trino
- **EMR clusters**: Monitor step execution and failure rates
- **S3 storage**: Track storage growth and access patterns

The alarm configurations provided will work with real metrics once you update the table names and adjust thresholds to match your environment.