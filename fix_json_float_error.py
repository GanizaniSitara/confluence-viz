"""
Helper functions to fix JSON serialization issues with float values.
Use these in your API endpoint that's failing.
"""

import json
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Union


def clean_nan_infinity(obj: Any) -> Any:
    """
    Recursively clean NaN and Infinity values from nested data structures.
    Replaces NaN with None, Infinity with 'Infinity' string.
    """
    if isinstance(obj, float):
        if np.isnan(obj):
            return None
        elif np.isinf(obj):
            return "Infinity" if obj > 0 else "-Infinity"
        return obj
    elif isinstance(obj, dict):
        return {k: clean_nan_infinity(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_infinity(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(clean_nan_infinity(item) for item in obj)
    elif isinstance(obj, np.ndarray):
        return clean_nan_infinity(obj.tolist())
    elif isinstance(obj, pd.DataFrame):
        # Replace NaN and inf values in DataFrame
        df_clean = obj.replace([np.inf, -np.inf], np.nan)
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        return df_clean.to_dict(orient='records')
    elif isinstance(obj, pd.Series):
        return clean_nan_infinity(obj.to_dict())
    return obj


class NanInfinityJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that handles NaN and Infinity values.
    """
    def encode(self, o):
        if isinstance(o, float):
            if np.isnan(o):
                return "null"
            elif np.isinf(o):
                return '"Infinity"' if o > 0 else '"-Infinity"'
        return super().encode(o)
    
    def iterencode(self, o, _one_shot=False):
        """Encode the given object and yield each string representation as available."""
        for chunk in super().iterencode(clean_nan_infinity(o), _one_shot):
            yield chunk


# Example usage in FastAPI endpoint:
"""
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import json

app = FastAPI()

@app.get("/aggregated-data")
async def get_aggregated_data():
    # Your existing code that produces data with potential NaN/Infinity
    data = your_data_processing_function()
    
    # Clean the data before returning
    cleaned_data = clean_nan_infinity(data)
    
    # Option 1: Return using the custom encoder
    return JSONResponse(
        content=json.loads(json.dumps(cleaned_data, cls=NanInfinityJSONEncoder))
    )
    
    # Option 2: Or just return the cleaned data directly
    # return cleaned_data
"""

# For pandas DataFrames specifically:
def clean_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a pandas DataFrame for JSON serialization.
    """
    # Replace infinity values with NaN first
    df = df.replace([np.inf, -np.inf], np.nan)
    
    # Fill NaN values - you can customize this based on your needs
    # Option 1: Replace with None (will become null in JSON)
    df = df.where(pd.notnull(df), None)
    
    # Option 2: Replace with specific values based on column type
    # for col in df.columns:
    #     if df[col].dtype in ['float64', 'float32']:
    #         df[col] = df[col].fillna(0.0)
    #     elif df[col].dtype == 'object':
    #         df[col] = df[col].fillna('')
    
    return df


# Test the functions
if __name__ == "__main__":
    # Test data with problematic values
    test_data = {
        "normal": 1.5,
        "nan_value": float('nan'),
        "inf_value": float('inf'),
        "neg_inf": float('-inf'),
        "nested": {
            "array": [1, float('nan'), 3, float('inf')],
            "more_nan": float('nan')
        }
    }
    
    print("Original data:", test_data)
    print("\nCleaned data:", clean_nan_infinity(test_data))
    print("\nJSON encoded:", json.dumps(clean_nan_infinity(test_data)))
    
    # Test with pandas DataFrame
    df = pd.DataFrame({
        'A': [1, 2, np.nan, 4],
        'B': [5, np.inf, 7, 8],
        'C': [9, 10, 11, -np.inf]
    })
    
    print("\nOriginal DataFrame:")
    print(df)
    print("\nCleaned DataFrame:")
    print(clean_dataframe_for_json(df))