import os
import csv
import json
import boto3
import time
from io import StringIO

s3 = boto3.client('s3')

_cache = {'data': None, 'ts': 0}
CACHE_TTL = 300  # 5 minutes

CORS_HEADERS = {
    'Access-Control-Allow-Origin': os.environ.get('ALLOWED_ORIGIN', '*'),
    'Access-Control-Allow-Methods': 'GET,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json',
}


def _load_jobs():
    bucket = os.environ['GOLD_BUCKET']
    key = os.environ.get('GOLD_KEY', 'all_jobs.csv')
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj['Body'].read().decode('utf-8')
    return list(csv.DictReader(StringIO(content)))


def _is_options(event):
    # API Gateway v2 (HTTP API) uses requestContext.http.method
    # API Gateway v1 (REST API) uses httpMethod
    method = (
        event.get('requestContext', {}).get('http', {}).get('method') or
        event.get('httpMethod') or ''
    )
    return method.upper() == 'OPTIONS'


def lambda_handler(event, context):
    if _is_options(event):
        return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': ''}

    # Support both API GW v1 (queryStringParameters) and v2 (same key)
    params = event.get('queryStringParameters') or {}
    search    = (params.get('search') or '').lower().strip()
    source    = (params.get('source') or '').lower().strip()
    remote    = (params.get('remote') or '').lower().strip()
    job_type  = (params.get('job_type') or '').lower().strip()
    location  = (params.get('location') or '').lower().strip()
    limit     = min(int(params.get('limit', 500)), 1000)

    now = time.time()
    if _cache['data'] is None or (now - _cache['ts']) > CACHE_TTL:
        _cache['data'] = _load_jobs()
        _cache['ts'] = now

    jobs = _cache['data']

    if search:
        jobs = [j for j in jobs if
                search in j.get('title', '').lower() or
                search in j.get('company', '').lower() or
                search in j.get('location', '').lower()]
    if location:
        jobs = [j for j in jobs if location in j.get('location', '').lower()]
    if source:
        jobs = [j for j in jobs if j.get('source', '').lower() == source]
    if remote == 'true':
        jobs = [j for j in jobs if j.get('is_remote', '').lower() == 'true']
    elif remote == 'false':
        jobs = [j for j in jobs if j.get('is_remote', '').lower() != 'true']
    if job_type:
        jobs = [j for j in jobs if job_type in j.get('job_types', '').lower()]

    all_jobs = _cache['data']
    today = __import__('datetime').date.today().isoformat()
    kpis = {
        'total':     len(all_jobs),
        'new_today': sum(1 for j in all_jobs if j.get('date_added', '') == today),
        'remote':    sum(1 for j in all_jobs if j.get('is_remote', '').lower() == 'true'),
        'filtered':  len(jobs),
    }

    return {
        'statusCode': 200,
        'headers': CORS_HEADERS,
        'body': json.dumps({'jobs': jobs[:limit], 'kpis': kpis, 'cached_at': _cache['ts']}),
    }
