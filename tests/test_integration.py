import pytest
import json
import boto3
from moto import mock_aws
from io import BytesIO
from src.lambda_function import lambda_handler
from src.process_data import ProcessDataEngine

class TestIntegration:
    
    @mock_aws
    def test_end_to_end_s3_processing(self):
        """
        Integration test simulating S3 event triggering Lambda.
        Uses moto to mock AWS services locally.
        """
        # Setup
        s3_client = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'test-integration-bucket'
        key_name = 'data/input.json'
        
        # Create Bucket
        s3_client.create_bucket(Bucket=bucket_name)
        
        # Put Object
        data = {
            "records": [
                {"id": 101, "name": "IntegrationTest", "value": 999}
            ]
        }
        s3_client.put_object(Bucket=bucket_name, Key=key_name, Body=json.dumps(data))
        
        # Construct Event
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": bucket_name},
                        "object": {"key": key_name}
                    }
                }
            ]
        }
        
        # Execute Lambda Handler
        # Note: In a real integration test, we might invoke the Lambda via boto3 client.invoke
        # Here we call the handler directly but with real-ish S3 backend via moto
        response = lambda_handler(event, None)
        
        # Assert
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Processing successful'
        
        # Verify data was processed (we can't easily check side effects without saving back to S3,
        # but we verified the handler completed successfully with valid S3 interaction)

    @mock_aws
    def test_processing_with_missing_s3_object(self):
        """
        Test how the system handles an S3 event for a non-existent object.
        """
        bucket_name = 'test-missing-obj-bucket'
        key_name = 'data/missing.json'
        
        # Create Bucket but DO NOT put object
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=bucket_name)
        
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": bucket_name},
                        "object": {"key": key_name}
                    }
                }
            ]
        }
        
        response = lambda_handler(event, None)
        
        # Should fail with 500 because get_object will raise NoSuchKey
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert 'error' in body