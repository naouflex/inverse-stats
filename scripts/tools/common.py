import pandas as pd 

def df_to_result(df: pd.DataFrame) -> dict:
    return {
        "columns": [{"name": col, "type": "float", "friendly_name": col} for col in df.columns],
        "rows": df.to_dict(orient='records')
    }