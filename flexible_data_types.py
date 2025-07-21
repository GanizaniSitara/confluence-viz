"""
Flexible data type handling for modern applications.
Move away from strict float requirements to accept any data type.
"""

from typing import Any, Union, List, Dict, Optional
import json
from decimal import Decimal
import pandas as pd
import numpy as np


class FlexibleDataHandler:
    """
    Handle various data types flexibly without strict type requirements.
    """
    
    @staticmethod
    def to_numeric_if_possible(value: Any) -> Union[float, int, Any]:
        """
        Convert to numeric type if possible, otherwise return original value.
        Preserves integers as integers, only converts to float when necessary.
        """
        if value is None or value == '':
            return None
            
        # Already numeric
        if isinstance(value, (int, float, np.integer, np.floating)):
            return value
            
        # Try conversion
        if isinstance(value, str):
            value = value.strip()
            try:
                # Try integer first to preserve type
                if '.' not in value and 'e' not in value.lower():
                    return int(value)
                else:
                    return float(value)
            except ValueError:
                return value  # Return original if not numeric
                
        return value
    
    @staticmethod
    def safe_divide(numerator: Any, denominator: Any, default: Any = None) -> Any:
        """
        Safely divide two values, handling type conversion and zero division.
        Returns None or default value on error.
        """
        try:
            num = FlexibleDataHandler.to_numeric_if_possible(numerator)
            den = FlexibleDataHandler.to_numeric_if_possible(denominator)
            
            if isinstance(num, (int, float)) and isinstance(den, (int, float)):
                if den == 0:
                    return default
                return num / den
            return default
        except:
            return default
    
    @staticmethod
    def safe_average(values: List[Any], skip_non_numeric: bool = True) -> Optional[float]:
        """
        Calculate average of a list, handling mixed types.
        """
        if not values:
            return None
            
        numeric_values = []
        for v in values:
            num_v = FlexibleDataHandler.to_numeric_if_possible(v)
            if isinstance(num_v, (int, float)):
                numeric_values.append(num_v)
            elif not skip_non_numeric:
                return None  # Can't average non-numeric values
                
        if not numeric_values:
            return None
            
        return sum(numeric_values) / len(numeric_values)
    
    @staticmethod
    def prepare_for_json(data: Any) -> Any:
        """
        Prepare any data structure for JSON serialization.
        Handles special cases like NaN, Infinity, numpy types, etc.
        """
        if isinstance(data, (np.integer, np.floating)):
            # Convert numpy types to Python types
            if np.isnan(data):
                return None
            elif np.isinf(data):
                return str(data)  # "inf" or "-inf"
            return data.item()
            
        elif isinstance(data, np.ndarray):
            return FlexibleDataHandler.prepare_for_json(data.tolist())
            
        elif isinstance(data, (list, tuple)):
            return [FlexibleDataHandler.prepare_for_json(item) for item in data]
            
        elif isinstance(data, dict):
            return {k: FlexibleDataHandler.prepare_for_json(v) for k, v in data.items()}
            
        elif isinstance(data, float):
            if np.isnan(data):
                return None
            elif np.isinf(data):
                return str(data)
            return data
            
        elif isinstance(data, Decimal):
            return float(data)
            
        elif pd.api.types.is_scalar(data) and pd.isna(data):
            return None
            
        else:
            return data
    
    @staticmethod
    def process_dataframe_flexibly(df: pd.DataFrame) -> pd.DataFrame:
        """
        Process DataFrame with flexible type handling.
        Infer and convert types intelligently.
        """
        df_processed = df.copy()
        
        for col in df_processed.columns:
            # Try to infer better types
            df_processed[col] = df_processed[col].apply(FlexibleDataHandler.to_numeric_if_possible)
            
            # Convert columns that are all numeric to appropriate dtype
            if df_processed[col].apply(lambda x: isinstance(x, (int, float, type(None)))).all():
                # Check if all non-null values are integers
                non_null = df_processed[col].dropna()
                if len(non_null) > 0 and all(isinstance(x, int) for x in non_null):
                    df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').astype('Int64')
                else:
                    df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
        
        return df_processed


# Example: Migrating from strict float handling to flexible handling

def old_way_calculate_average(values: List[float]) -> float:
    """Old way: Expects floats, will crash on other types."""
    return sum(values) / len(values)


def new_way_calculate_average(values: List[Any]) -> Optional[Union[int, float]]:
    """New way: Accepts any types, handles gracefully."""
    return FlexibleDataHandler.safe_average(values)


# Example usage for API endpoints
def flexible_api_response(data: Any) -> Dict:
    """
    Prepare any data for API response with flexible type handling.
    """
    # Process the data flexibly
    if isinstance(data, pd.DataFrame):
        data = FlexibleDataHandler.process_dataframe_flexibly(data)
        data_dict = data.to_dict(orient='records')
    else:
        data_dict = data
    
    # Prepare for JSON serialization
    clean_data = FlexibleDataHandler.prepare_for_json(data_dict)
    
    return {
        "status": "success",
        "data": clean_data,
        "metadata": {
            "flexible_types": True,
            "null_handling": "converted_to_none",
            "special_values_handling": "converted_to_strings"
        }
    }


# Migration examples
if __name__ == "__main__":
    # Test mixed type data
    mixed_data = [1, 2.5, "3", "4.5", None, "not a number", float('inf'), float('nan')]
    
    print("Original data:", mixed_data)
    print("Average (old way would crash):", new_way_calculate_average(mixed_data))
    print("Prepared for JSON:", FlexibleDataHandler.prepare_for_json(mixed_data))
    
    # Test DataFrame with mixed types
    df = pd.DataFrame({
        'A': [1, "2", 3.5, None, "not a number"],
        'B': ["10", 20, "30.5", float('inf'), None],
        'C': ["text", "more text", "123", "456.78", "end"]
    })
    
    print("\nOriginal DataFrame:")
    print(df)
    print("\nProcessed DataFrame:")
    processed = FlexibleDataHandler.process_dataframe_flexibly(df)
    print(processed)
    print("\nDtypes:", processed.dtypes)