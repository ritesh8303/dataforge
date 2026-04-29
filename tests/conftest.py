import pytest

import json
import boto3
from moto import mock_aws

from io import BytesIO

@pytest.fixture
def sample_input_data():
    """Provides sample JSON data for testing."""
    return {
        "records": [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 300}
        ]
    }

@pytest.fixture
def sample_csv_content():
    """Provides sample CSV content string."""
    return "id,name,value\n1,Alice,100\n2,Bob,200\n"

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture
def s3_client(aws_credentials):
    """Create a S3 client for testing."""
    with mock_aws():
        yield boto3.client('s3', region_name='us-east-1')