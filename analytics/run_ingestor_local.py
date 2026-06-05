import os
import sys
import argparse

# Add src/ folder to python path so we can import ingestors
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

INGESTORS = {
    "ba": ("ingest_ba_api", "BA API Ingestor"),
    "arbeitnow": ("ingest_arbeitnow", "Arbeitnow Ingestor"),
    "hn": ("ingest_hn", "Hacker News Ingestor"),
    "berlin": ("ingest_berlin_startups", "Berlin Startup Jobs Ingestor"),
    "apify": ("ingest_apify", "Apify LinkedIn/Indeed Ingestor"),
    "direct": ("ingest_company_careers", "Direct Company Careers Ingestor"),
}

def main():
    parser = argparse.ArgumentParser(description="Run DataForge ingestors locally in sandbox mode.")
    parser.add_argument(
        "ingestor",
        choices=list(INGESTORS.keys()),
        help=f"The key of the ingestor to run: {', '.join(INGESTORS.keys())}"
    )
    args = parser.parse_args()

    # Set LOCAL_RUN environment variable to enable dual S3/local mode fallback
    os.environ["LOCAL_RUN"] = "true"

    module_name, display_name = INGESTORS[args.ingestor]
    print(f"=== Running {display_name} locally ===")
    
    try:
        # Dynamically import the ingestor module
        module = __import__(module_name)
        
        # Invoke its lambda_handler
        result = module.lambda_handler({}, None)
        
        print("\n=== Execution Completed successfully ===")
        print(f"Status Code: {result.get('statusCode')}")
        print(f"Body: {result.get('body')}")
        
    except Exception as e:
        print("\n=== Execution Failed ===")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
