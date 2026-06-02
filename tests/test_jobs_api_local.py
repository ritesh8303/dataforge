import os
import json
import sys
from unittest.mock import patch, MagicMock

# Add src to python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Set dummy environment variable before import
os.environ['GOLD_BUCKET'] = 'test-gold-bucket'

from jobs_api import lambda_handler

# Sample mock CSV data matching DataForge Gold format
MOCK_CSV = """job_id,title,company,location,tags,is_remote,date_added,job_types
1,Senior Data Scientist,Tech Corp,Munich,AI / ML,false,2026-06-01,full_time
2,Data Engineer,Data Corp,Berlin,"Junior / Entry Level,Data Engineering",true,2026-06-02,full_time
3,Working Student - Data Analyst,AI Analytics,Hamburg,"Working Student,Analytics / BI",false,2026-06-02,part_time
4,Intern Data Analyst,BI Corp,Cologne,"Internship,Analytics / BI",true,2026-06-02,internship
5,Master Thesis - Deep Learning,Research Lab,Munich,"Master Thesis,AI / ML",false,2026-06-02,thesis
"""

@patch('jobs_api.s3')
def test_jobs_api_experience_filters(mock_s3):
    # Setup mock S3 response
    mock_obj = MagicMock()
    mock_obj['Body'].read.return_value = MOCK_CSV.encode('utf-8')
    mock_s3.get_object.return_value = mock_obj

    # Test 1: No filters (should return all 5 jobs)
    event = {"queryStringParameters": {}}
    res = lambda_handler(event, None)
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert len(body['jobs']) == 5

    # Test 2: Search "junior" in tags (should return job 2)
    # Note: job 2 doesn't have "junior" in title, only in tags
    event = {"queryStringParameters": {"search": "junior"}}
    res = lambda_handler(event, None)
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert len(body['jobs']) == 1
    assert body['jobs'][0]['job_id'] == '2'

    # Test 3: Filter by experience=junior
    event = {"queryStringParameters": {"experience": "junior"}}
    res = lambda_handler(event, None)
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert len(body['jobs']) == 1
    assert body['jobs'][0]['job_id'] == '2'

    # Test 4: Filter by experience=student
    event = {"queryStringParameters": {"experience": "student"}}
    res = lambda_handler(event, None)
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert len(body['jobs']) == 1
    assert body['jobs'][0]['job_id'] == '3'

    # Test 5: Filter by experience=intern
    event = {"queryStringParameters": {"experience": "intern"}}
    res = lambda_handler(event, None)
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert len(body['jobs']) == 1
    assert body['jobs'][0]['job_id'] == '4'

    # Test 6: Filter by experience=thesis
    event = {"queryStringParameters": {"experience": "thesis"}}
    res = lambda_handler(event, None)
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert len(body['jobs']) == 1
    assert body['jobs'][0]['job_id'] == '5'

    print("All experience filter tests passed successfully!")

if __name__ == '__main__':
    test_jobs_api_experience_filters()
