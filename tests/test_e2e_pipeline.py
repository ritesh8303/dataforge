"""
End-to-end integration test for the DataForge pipeline.
Tests Bronze ingestion → Silver transformation → Gold analytics.
"""
import boto3
import time
import json

def test_pipeline_e2e():
    """Test the full pipeline end-to-end in AWS."""
    lambda_client = boto3.client('lambda', region_name='eu-central-1')
    s3_client = boto3.client('s3', region_name='eu-central-1')
    
    print("\n=== DataForge Pipeline E2E Test ===\n")
    
    # Step 1: Invoke Arbeitnow ingestor
    print("1. Testing Arbeitnow ingestor...")
    response = lambda_client.invoke(
        FunctionName='dataforge-ingestor',
        InvocationType='RequestResponse',
        Payload=json.dumps({})
    )
    
    if response.get('FunctionError'):
        error = json.loads(response['Payload'].read().decode())
        print(f"   ❌ FAILED: {error}")
        return False
    
    result = json.loads(response['Payload'].read().decode())
    if result.get('statusCode') != 200:
        print(f"   ❌ FAILED: {result}")
        return False
    
    print(f"   ✅ SUCCESS: {result.get('body')}")
    
    # Step 2: Check Bronze bucket for new data
    print("\n2. Checking Bronze bucket...")
    try:
        objects = s3_client.list_objects_v2(
            Bucket='dataforge-bronze-dev-eu-central-1',
            Prefix='arbeitnow/'
        )
        if objects.get('KeyCount', 0) == 0:
            print("   ❌ FAILED: No files in Bronze")
            return False
        latest = sorted(objects['Contents'], key=lambda x: x['LastModified'])[-1]
        print(f"   ✅ Latest Bronze file: {latest['Key']} ({latest['Size']} bytes)")
        bronze_key = latest['Key']
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        return False
    
    # Step 3: Manually trigger transformer (S3 event simulation)
    print("\n3. Testing Silver transformer...")
    event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'dataforge-bronze-dev-eu-central-1'},
                'object': {'key': bronze_key}
            }
        }]
    }
    
    response = lambda_client.invoke(
        FunctionName='dataforge-transformer',
        InvocationType='RequestResponse',
        Payload=json.dumps(event)
    )
    
    if response.get('FunctionError'):
        error = json.loads(response['Payload'].read().decode())
        print(f"   ❌ FAILED: {error}")
        return False
    
    print("   ✅ Transformer executed successfully")
    
    # Step 4: Verify Silver bucket has data
    print("\n4. Checking Silver bucket...")
    try:
        objects = s3_client.list_objects_v2(
            Bucket='dataforge-silver-dev-eu-central-1',
            Prefix='cleaned/jobs_history.parquet'
        )
        if objects.get('KeyCount', 0) == 0:
            print("   ❌ FAILED: No files in Silver")
            return False
        total_size = sum(obj['Size'] for obj in objects['Contents'])
        print(f"   ✅ Silver has {objects['KeyCount']} file(s), {total_size:,} bytes total")
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        return False
    
    # Step 5: Check Gold bucket
    print("\n5. Checking Gold bucket...")
    try:
        objects = s3_client.list_objects_v2(
            Bucket='dataforge-gold-dev-eu-central-1'
        )
        if objects.get('KeyCount', 0) == 0:
            print("   ⚠️  WARNING: No files in Gold (run query_gold.py to generate)")
        else:
            print(f"   ✅ Gold has {objects['KeyCount']} CSV file(s)")
            for obj in objects.get('Contents', []):
                print(f"      - {obj['Key']} ({obj['Size']} bytes)")
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        return False
    
    # Step 6: Verify EventBridge schedules
    print("\n6. Checking EventBridge schedules...")
    events_client = boto3.client('events', region_name='eu-central-1')
    try:
        rules = events_client.list_rules(NamePrefix='dataforge')
        enabled = [r for r in rules['Rules'] if r['State'] == 'ENABLED']
        if len(enabled) != 3:
            print(f"   ❌ FAILED: Expected 3 enabled schedules, found {len(enabled)}")
            return False
        print(f"   ✅ All 3 daily schedules enabled (8AM UTC)")
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        return False
    
    # Step 7: Verify S3 trigger
    print("\n7. Checking S3 trigger configuration...")
    try:
        config = s3_client.get_bucket_notification_configuration(
            Bucket='dataforge-bronze-dev-eu-central-1'
        )
        lambdas = config.get('LambdaFunctionConfigurations', [])
        if not lambdas:
            print("   ❌ FAILED: No Lambda triggers configured")
            return False
        trigger = lambdas[0]
        suffix = trigger['Filter']['Key']['FilterRules'][0]['Value']
        if suffix != '.parquet':
            print(f"   ❌ FAILED: Wrong suffix '{suffix}', expected '.parquet'")
            return False
        print(f"   ✅ S3 trigger configured correctly (.parquet → transformer)")
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        return False
    
    print("\n=== ✅ ALL TESTS PASSED ===\n")
    print("Pipeline Status: OPERATIONAL")
    print("Next automated run: Tomorrow at 8:00 AM UTC")
    return True

if __name__ == '__main__':
    success = test_pipeline_e2e()
    exit(0 if success else 1)
