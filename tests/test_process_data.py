import pytest
import pandas as pd
import json
from io import StringIO

# Ensure you run pytest from the project root (dataforge/) so 'src' is discoverable
from src.process_data import ProcessDataEngine

class TestProcessDataEngine:
    
    def setup_method(self):
        """Initialize the engine before each test."""
        self.engine = ProcessDataEngine()

    def test_load_json_data(self, sample_input_data):
        """Test loading data from a JSON dictionary."""
        df = self.engine.load_data(sample_input_data, format_type='json')
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ['id', 'name', 'value']

    def test_load_csv_data(self, sample_csv_content):
        """Test loading data from a CSV string."""
        df = self.engine.load_data(sample_csv_content, format_type='csv')
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'name' in df.columns

    def test_clean_data_removes_nulls(self):
        """Test that cleaning removes rows with null values."""
        data = {
            "records": [
                {"id": 1, "name": "Alice", "value": 100},
                {"id": 2, "name": None, "value": 200},
                {"id": 3, "name": "Charlie", "value": None}
            ]
        }
        df = self.engine.load_data(data, format_type='json')
        cleaned_df = self.engine.clean_data(df)
        
        # Assuming default dropna behavior
        assert len(cleaned_df) < len(df)

    def test_transform_data_adds_columns(self, sample_input_data):
        """Test that transformation adds expected columns."""
        df = self.engine.load_data(sample_input_data, format_type='json')
        transformed_df = self.engine.transform_data(df)
        
        assert 'processed_at' in transformed_df.columns
        assert 'id' in transformed_df.columns

    def test_full_pipeline_execution(self, sample_input_data):
        """Test the end-to-end pipeline execution."""
        df = self.engine.load_data(sample_input_data, format_type='json')
        cleaned_df = self.engine.clean_data(df)
        final_df = self.engine.transform_data(cleaned_df)
        
        assert not final_df.empty
        assert len(final_df) > 0

    def test_handle_empty_data(self):
        """Test handling of empty input data."""
        empty_data = {"records": []}
        df = self.engine.load_data(empty_data, format_type='json')
        assert df.empty

    def test_load_unsupported_format(self):
        """Test that an unsupported format raises ValueError."""
        with pytest.raises(ValueError) as excinfo:
            self.engine.load_data({}, format_type='xml')
        assert "Unsupported format type: xml" in str(excinfo.value)

    def test_transform_data_immutability(self, sample_input_data):
        """Test that transform_data does not modify the original DataFrame."""
        df = self.engine.load_data(sample_input_data, format_type='json')
        original_columns = list(df.columns)
        
        transformed_df = self.engine.transform_data(df)
        
        # Original dataframe should not have the new column
        assert 'processed_at' not in df.columns
        assert list(df.columns) == original_columns
        
        # Transformed dataframe should have the new column
        assert 'processed_at' in transformed_df.columns

    def test_clean_data_empty_dataframe(self):
        """Test cleaning an already empty or all-null dataframe."""
        data = {"records": []}
        df = self.engine.load_data(data, format_type='json')
        cleaned_df = self.engine.clean_data(df)
        assert cleaned_df.empty
