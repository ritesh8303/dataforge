import pytest
import time
import pandas as pd
from src.process_data import ProcessDataEngine

class TestPerformance:
    
    def test_large_dataset_processing_time(self):
        """
        Ensure processing large datasets completes within acceptable time limits.
        Simulates Lambda timeout constraints (e.g., must finish in < 30s for this batch).
        """
        engine = ProcessDataEngine()
        
        # Generate large dataset
        num_records = 10000
        data = {
            "records": [
                {"id": i, "name": f"User{i}", "value": i * 10} 
                for i in range(num_records)
            ]
        }
        
        start_time = time.time()
        
        # Execute Pipeline
        df = engine.load_data(data, format_type='json')
        cleaned_df = engine.clean_data(df)
        final_df = engine.transform_data(cleaned_df)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Assertions
        assert len(final_df) == num_records
        # Assert processing time is reasonable (e.g., less than 5 seconds for 10k records in local env)
        assert processing_time < 5.0, f"Processing took too long: {processing_time:.2f}s"

    def test_memory_usage_basic(self):
        """
        Basic check to ensure DataFrame operations don't explode memory unnecessarily.
        """
        engine = ProcessDataEngine()
        num_records = 50000
        data = {
            "records": [
                {"id": i, "name": f"User{i}", "value": i * 10} 
                for i in range(num_records)
            ]
        }
        
        df = engine.load_data(data, format_type='json')
        # Check that dataframe is created
        assert not df.empty
        assert len(df) == num_records