# Guide: Fixing JSON Float Serialization Errors in API Endpoints

## Problem Description

You're experiencing the error:
```
ValueError: Out of range float values are not JSON compliant
```

This occurs when your API tries to return data containing:
- `NaN` (Not a Number) values
- `Infinity` or `-Infinity` values
- NumPy or Pandas special float types

## Quick Fix (Immediate Solution)

### 1. Find your `/aggregated-data` endpoint

Look for something like:
```python
@app.get("/aggregated-data")
async def get_aggregated_data():
    # Your data processing code
    data = process_data()
    return data
```

### 2. Add the cleaning function

Add this helper function to your API file:

```python
import numpy as np
import pandas as pd
from typing import Any

def clean_for_json(obj: Any) -> Any:
    """Recursively clean NaN and Infinity values from data."""
    if isinstance(obj, float):
        if np.isnan(obj):
            return None
        elif np.isinf(obj):
            return None  # or "Infinity" if you want to preserve the information
        return obj
    elif isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj]
    elif isinstance(obj, np.ndarray):
        return clean_for_json(obj.tolist())
    elif isinstance(obj, pd.DataFrame):
        # Replace NaN and inf values in DataFrame
        df_clean = obj.replace([np.inf, -np.inf], np.nan)
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        return df_clean.to_dict(orient='records')
    elif isinstance(obj, pd.Series):
        return clean_for_json(obj.to_dict())
    return obj
```

### 3. Update your endpoint

```python
@app.get("/aggregated-data")
async def get_aggregated_data():
    # Your existing data processing code
    data = process_data()
    
    # Clean the data before returning
    cleaned_data = clean_for_json(data)
    return cleaned_data
```

## Long-term Solution (Flexible Data Types)

### 1. Create a flexible data handler module

Create a new file `flexible_data_handler.py`:

```python
from typing import Any, Union, List, Dict, Optional
import pandas as pd
import numpy as np

class FlexibleDataHandler:
    @staticmethod
    def to_numeric_if_possible(value: Any) -> Union[float, int, Any]:
        """Convert to numeric type if possible, otherwise return original."""
        if value is None or value == '':
            return None
            
        if isinstance(value, (int, float, np.integer, np.floating)):
            return value
            
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
        """Safely divide two values, handling type conversion and zero division."""
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
    def prepare_for_json(data: Any) -> Any:
        """Prepare any data structure for JSON serialization."""
        if isinstance(data, (np.integer, np.floating)):
            if np.isnan(data):
                return None
            elif np.isinf(data):
                return str(data)
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
            
        elif pd.api.types.is_scalar(data) and pd.isna(data):
            return None
            
        else:
            return data
```

### 2. Update your API endpoints

```python
from flexible_data_handler import FlexibleDataHandler

@app.get("/aggregated-data")
async def get_aggregated_data():
    # Your data processing - now can handle mixed types
    raw_data = get_data_from_source()
    
    # Process with flexible type handling
    if isinstance(raw_data, pd.DataFrame):
        # DataFrames often have mixed types and special float values
        processed_data = raw_data.to_dict(orient='records')
    else:
        processed_data = raw_data
    
    # Prepare for JSON serialization
    clean_data = FlexibleDataHandler.prepare_for_json(processed_data)
    
    return {
        "status": "success",
        "data": clean_data
    }
```

## Common Scenarios and Solutions

### Scenario 1: DataFrame with calculations that produce NaN

```python
# Problem code
df['ratio'] = df['numerator'] / df['denominator']  # Produces inf if denominator is 0
df['average'] = df.groupby('category')['value'].transform('mean')  # Produces NaN for empty groups

# Solution
df['ratio'] = df.apply(
    lambda row: FlexibleDataHandler.safe_divide(row['numerator'], row['denominator'], default=0),
    axis=1
)
df['average'] = df.groupby('category')['value'].transform(lambda x: x.mean() if len(x) > 0 else None)
```

### Scenario 2: Mixed data types from CSV/Excel

```python
# Problem: CSV might have mixed types
df = pd.read_csv('data.csv')  # Might have strings, numbers, empty cells

# Solution
df = pd.read_csv('data.csv', na_values=['N/A', 'NULL', ''])
for col in df.columns:
    df[col] = df[col].apply(FlexibleDataHandler.to_numeric_if_possible)
```

### Scenario 3: Aggregations with empty data

```python
# Problem
result = {
    'mean': data.mean(),  # NaN if data is empty
    'sum': data.sum(),    # 0 if empty, but might want None
    'max': data.max()     # NaN if empty
}

# Solution
result = {
    'mean': data.mean() if len(data) > 0 else None,
    'sum': data.sum() if len(data) > 0 else None,
    'max': data.max() if len(data) > 0 else None
}
# Then clean for JSON
result = FlexibleDataHandler.prepare_for_json(result)
```

## Testing Your Changes

### 1. Test with problematic data

```python
# Create test data with edge cases
test_data = {
    'normal': 1.5,
    'nan_value': float('nan'),
    'inf_value': float('inf'),
    'neg_inf': float('-inf'),
    'zero_division': 1/0 if False else None,
    'empty_mean': pd.Series([]).mean(),
    'mixed_types': [1, "2", 3.5, None, "text"]
}

# Test your endpoint
cleaned = FlexibleDataHandler.prepare_for_json(test_data)
print(json.dumps(cleaned))  # Should work without errors
```

### 2. Add logging to identify issues

```python
import logging

@app.get("/aggregated-data")
async def get_aggregated_data():
    try:
        data = process_data()
        
        # Log data types for debugging
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, float):
                    if np.isnan(value) or np.isinf(value):
                        logging.warning(f"Found problematic float in {key}: {value}")
        
        cleaned_data = FlexibleDataHandler.prepare_for_json(data)
        return cleaned_data
        
    except ValueError as e:
        logging.error(f"JSON serialization error: {e}")
        logging.error(f"Data sample: {str(data)[:200]}")
        raise
```

## Migration Checklist

- [ ] Identify all endpoints returning numeric data
- [ ] Add the cleaning function to your codebase
- [ ] Update endpoints to clean data before returning
- [ ] Test with edge cases (empty data, division by zero, etc.)
- [ ] Add error logging to track issues
- [ ] Consider using flexible data types for new features
- [ ] Update data processing to handle mixed types gracefully

## Benefits of Flexible Type Handling

1. **Robustness**: Won't crash on unexpected data types
2. **User-friendly**: Handles common data issues automatically
3. **Maintainable**: Centralized type handling logic
4. **Future-proof**: Adapts to changing data sources
5. **Better debugging**: Clear handling of edge cases

## Additional Resources

- [JSON specification](https://www.json.org/) - Understanding JSON limitations
- [Pandas nullable integer types](https://pandas.pydata.org/docs/user_guide/integer_na.html)
- [NumPy special values](https://numpy.org/doc/stable/reference/constants.html)

Remember: The goal is to make your API resilient to real-world data while maintaining data integrity where it matters.