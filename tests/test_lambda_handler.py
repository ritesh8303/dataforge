import pytest
import json
from unittest.mock import patch, MagicMock
from io import BytesIO

# Add the parent directory to the path

# Ensure you run pytest from the project root (dataforge/) so 'src' is discoverable
from src.lambda_function import lambda_handler

class TestLambdaHandler:
    
    @patch('src.lambda_function.ProcessDataEngine')
    @patch('src.lambda_function.boto3.client')
    def test_successful_processing(self, mock_boto_client, mock_engine_class):
        """Test successful execution of the Lambda handler."""
        
        # Mock the S3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock the ProcessDataEngine
        mock_engine = MagicMock()
        mock_engine_class.return_value = mock_engine
        
        # Mock DataFrame response for transform_data
        import pandas as pd
        mock_df = pd.DataFrame({'id': [1], 'name': ['Test']})
        mock_engine.load_data.return_value = mock_df
        mock_engine.clean_data.return_value = mock_df
        mock_engine.transform_data.return_value = mock_df
        
        # Mock event
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "test-key.json"}
                    }
                }
            ]
        }
        
        context = MagicMock()
        
        # Mock S3 get_object response to return valid JSON content
        mock_json_content = json.dumps({"records": [{"id": 1, "name": "Test", "value": 100}]})
        mock_s3.get_object.return_value = {
            'Body': BytesIO(mock_json_content.encode('utf-8'))
        }
        
        # Execute handler
        response = lambda_handler(event, context)
        
        # Assertions
        assert response['statusCode'] == 200
        mock_s3.get_object.assert_called_once_with(Bucket='test-bucket', Key='test-key.json')
        mock_engine.load_data.assert_called_once()

    def test_missing_records_in_event(self):
        """Test handling of event without Records."""
        event = {}
        context = MagicMock()
        
        response = lambda_handler(event, context)
        
        assert response['statusCode'] == 400
        assert 'error' in json.loads(response['body'])

    @patch('src.lambda_function.ProcessDataEngine')
    @patch('src.lambda_function.boto3.client')
    def test_lambda_exception_handling(self, mock_boto_client, mock_engine_class):
        """Test that exceptions in processing return a 500 error."""
        
        # Mock the S3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock the ProcessDataEngine to raise an exception during load
        mock_engine = MagicMock()
        mock_engine_class.return_value = mock_engine
        mock_engine.load_data.side_effect = Exception("Processing failed")
        
        # Mock event
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "test-key.json"}
                    }
                }
            ]
        }
        
        context = MagicMock()
        
        # Mock S3 get_object response
        mock_json_content = json.dumps({"records": [{"id": 1}]})
        mock_s3.get_object.return_value = {
            'Body': BytesIO(mock_json_content.encode('utf-8'))
        }
        
        # Execute handler
        response = lambda_handler(event, context)
        
        # Assertions
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert 'error' in body
        assert "Processing failed" in body['error']

    @patch('src.lambda_function.ProcessDataEngine')
    @patch('src.lambda_function.boto3.client')
    def test_invalid_json_in_s3_object(self, mock_boto_client, mock_engine_class):
        """Test handling of invalid JSON content in S3 object."""
        
        # Mock the S3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock event
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "test-key.json"}
                    }
                }
            ]
        }
        
        context = MagicMock()
        
        # Mock S3 get_object response to return invalid JSON content
        mock_s3.get_object.return_value = {
            'Body': BytesIO(b'{invalid json}')
        }
        
        # Execute handler
        response = lambda_handler(event, context)
        
        # Assertions
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert 'error' in body

    @patch('src.lambda_function.ProcessDataEngine')
    @patch('src.lambda_function.boto3.client')
    def test_empty_records_list(self, mock_boto_client, mock_engine_class):
        """Test handling of event with empty Records list."""
        
        # Mock the S3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock event with empty Records
        event = {
            "Records": []
        }
        
        context = MagicMock()
        
        # Execute handler
        response = lambda_handler(event, context)
        
        # Assertions - should succeed but process 0 records
        assert response['statusCode'] == 200
        mock_s3.get_object.assert_not_called()
        mock_engine_class.return_value.load_data.assert_not_called()

    @patch('src.lambda_function.ProcessDataEngine')
    @patch('src.lambda_function.boto3.client')
    def test_processing_multiple_records(self, mock_boto_client, mock_engine_class):
        """Test processing multiple S3 records in one event."""
        
        # Mock the S3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock the ProcessDataEngine
        mock_engine = MagicMock()
        mock_engine_class.return_value = mock_engine
        
        # Mock DataFrame response
        import pandas as pd
        mock_df = pd.DataFrame({'id': [1], 'name': ['Test']})
        mock_engine.load_data.return_value = mock_df
        mock_engine.clean_data.return_value = mock_df
        mock_engine.transform_data.return_value = mock_df
        
        # Mock event with two records
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "bucket1"},
                        "object": {"key": "key1.json"}
                    }
                },
                {
                    "s3": {
                        "bucket": {"name": "bucket2"},
                        "object": {"key": "key2.json"}
                    }
                }
            ]
        }
        
        context = MagicMock()
        
        # Mock S3 get_object response
        # Use side_effect to return a new BytesIO instance for each call
        # Otherwise, the same BytesIO buffer is consumed on the first read and empty on the second
        mock_json_content = json.dumps({"records": [{"id": 1}]})
        mock_s3.get_object.side_effect = [
            {'Body': BytesIO(mock_json_content.encode('utf-8'))},
            {'Body': BytesIO(mock_json_content.encode('utf-8'))}
        ]
        
        # Execute handler
        response = lambda_handler(event, context)
        
        # Assertions
        assert response['statusCode'] == 200
        assert mock_s3.get_object.call_count == 2
        assert mock_engine.load_data.call_count == 2

    @patch('src.lambda_function.ProcessDataEngine')
    @patch('src.lambda_function.boto3.client')
    def test_security_error_no_stack_trace_leak(self, mock_boto_client, mock_engine_class):
        """
        Security Test: Ensure that unexpected errors do not leak stack traces or internal paths.
        """
        # Mock the S3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock the ProcessDataEngine to raise a complex exception
        mock_engine = MagicMock()
        mock_engine_class.return_value = mock_engine
        
        # Raise an exception that might contain sensitive info if printed directly
        try:
            raise ValueError("Sensitive DB connection string: user=admin pass=secret")
        except Exception as e:
            mock_engine.load_data.side_effect = e
        
        # Mock event
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "test-key.json"}
                    }
                }
            ]
        }
        
        context = MagicMock()
        
        # Mock S3 get_object response
        mock_json_content = json.dumps({"records": [{"id": 1}]})
        mock_s3.get_object.return_value = {
            'Body': BytesIO(mock_json_content.encode('utf-8'))
        }
        
        # Execute handler
        response = lambda_handler(event, context)
        
        # Assertions
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert 'error' in body
        
        # Security Check: Ensure the error message doesn't contain obvious stack trace elements
        # like "File ", "line ", or ".py" which indicate internal structure leakage
        error_msg = body['error']
        assert "File " not in error_msg, "Error message leaks file path information"
        assert "line " not in error_msg, "Error message leaks line number information"
