import sys
import os
from pathlib import Path

# The src/ directory contains pydantic/requests/etc. bundled for Lambda deployment.
# These are Linux binaries and must NOT be imported during testing.
# We remove src/ from sys.path and purge any already-imported src/ modules
# so tests use the system-installed packages from requirements-test.txt instead.
import os
_src_normalized = os.path.normcase(os.path.abspath(Path(__file__).resolve().parents[1] / "src"))
sys.path = [p for p in sys.path if os.path.normcase(os.path.abspath(p)) != _src_normalized]

# Purge any src/-based modules already cached
_to_remove = [k for k, v in sys.modules.items()
              if hasattr(v, '__file__') and v.__file__ and os.path.normcase(os.path.abspath(v.__file__)).startswith(_src_normalized)]
for k in _to_remove:
    del sys.modules[k]

import pytest
import boto3
from moto import mock_aws

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