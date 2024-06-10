import pandas as pd 

def df_to_result(df: pd.DataFrame) -> dict:
    def get_type(col):
        if df[col].dtype == 'float64':
            return 'float'
        elif df[col].dtype == 'int64':
            return 'integer'
        else:
            return 'string'

    return {
        "columns": [{"name": col, "type": get_type(col), "friendly_name": col} for col in df.columns],
        "rows": df.fillna('').to_dict(orient='records')
    }