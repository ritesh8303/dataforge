import os
import sys
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

# Mock env vars
os.environ["GOLD_BUCKET"] = "local-mock"
os.environ["GOLD_KEY"] = "all_jobs.csv"

# Patch boto3 to read from local analytics/ folder instead of AWS S3
import boto3
from unittest.mock import MagicMock

local_gold_dir = os.path.dirname(__file__)


def mock_get_object(Bucket, Key):
    filepath = os.path.join(local_gold_dir, Key)
    if not os.path.exists(filepath):
        # Fallback to search in root analytics or docs
        filepath = os.path.join(os.path.dirname(__file__), "..", "analytics", Key)

    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    mock_body = MagicMock()
    mock_body.read.return_value = content.encode("utf-8")
    return {"Body": mock_body}


mock_s3 = MagicMock()
mock_s3.get_object = mock_get_object
boto3.client = lambda service, *args, **kwargs: mock_s3 if service == "s3" else MagicMock()

# Import the actual lambda_handlers
from jobs_api import lambda_handler as jobs_handler
from metrics_api import lambda_handler as metrics_handler


class LocalAPIHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed_url = urlparse(self.path)
        
        # Determine if request is for static HTML file in docs/
        filename = parsed_url.path.lstrip("/")
        if filename.startswith("docs/"):
            filename = filename[5:]
            
        docs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docs"))
        static_file_path = os.path.join(docs_dir, filename)
        
        # Serve default index.html for base paths
        if parsed_url.path in ("/", "/docs", "/docs/"):
            static_file_path = os.path.join(docs_dir, "index.html")
            
        if os.path.exists(static_file_path) and os.path.isfile(static_file_path) and static_file_path.endswith(".html"):
            try:
                with open(static_file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(content.encode("utf-8"))
                return
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(f"Error reading static file: {str(e)}".encode("utf-8"))
                return

        # Fallback to API handlers
        query_params = parse_qs(parsed_url.query)
        # Convert list values to single strings as API Gateway does
        single_params = {k: v[0] for k, v in query_params.items()}

        # Construct mock Lambda event
        event = {"queryStringParameters": single_params, "requestContext": {"http": {"method": "GET"}}}

        # Route to appropriate handler based on the path
        if parsed_url.path == "/metrics":
            handler = metrics_handler
        else:
            handler = jobs_handler

        # Invoke handler
        try:
            response = handler(event, None)
            self.send_response(response["statusCode"])
            has_cors = False
            for k, v in response.get("headers", {}).items():
                self.send_header(k, v)
                if k.lower() == "access-control-allow-origin":
                    has_cors = True
            if not has_cors:
                self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(response["body"].encode("utf-8"))
        except Exception as e:
            self.send_response(500)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))


def run(port=8000):
    server = HTTPServer(("localhost", port), LocalAPIHandler)
    print(f"Starting local API server on http://localhost:{port}...")
    print(f"Using local CSV data from: {os.path.abspath(os.path.join(local_gold_dir, 'all_jobs.csv'))}")
    print("Press Ctrl+C to stop.")
    server.serve_forever()


if __name__ == "__main__":
    run()
