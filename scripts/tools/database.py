import pandas as pd
import traceback
import sqlalchemy.exc as sqla_exc
import io
from sqlalchemy import (MetaData, Column, Integer, String, Float, DateTime, 
                        Table, select, create_engine)
import logging

logger = logging.getLogger(__name__)

def table_exists(db_url, table_name):
    engine = create_engine(db_url)
    return engine.dialect.has_table(engine.connect(), table_name)

def map_dtype(dtype):
    dtype_str = str(dtype)
    mapping = {
        'O': String,
        'object': String,
        'string': String,
        'int64': Integer,
        'float64': Float,
        'datetime64[ns]': DateTime,
        'Int64': Integer,
    }
    return mapping.get(dtype_str, String if "str" in dtype_str else Integer)

def create_table_from_df(engine, table_name, df):
    meta = MetaData()
    columns = [Column(col, map_dtype(df[col].dtype)) for col in df.columns]
    Table(table_name, meta, *columns).create(engine)

def save_table(db_url, table_name, df):
    engine = create_engine(db_url)
    if not table_exists(db_url, table_name):
        create_table_from_df(engine, table_name, df)
    df.to_sql(table_name, engine, if_exists='append', index=False)

def get_table(db_url, table_name):
    if table_exists(db_url, table_name):
        return pd.read_sql_table(table_name, create_engine(db_url))

def update_table(db_url, table_name, df):
    engine = create_engine(db_url)
    if not table_exists(db_url, table_name):
        return
    existing_df = get_table(db_url, table_name)
    if existing_df is not None:
        new_rows = df[~df.isin(existing_df.to_dict('list')).all(1)]
        save_table(db_url, table_name, new_rows)

def remove_duplicates(db_url, table_name, duplicate_columns=None, order_column=None):
    engine = create_engine(db_url)
    df = get_table(db_url, table_name)
    if df is not None:
        duplicate_columns = duplicate_columns or df.columns
        order_column = order_column or df.columns[0]
        df.sort_values(by=order_column, ascending=False, inplace=True)
        df.drop_duplicates(subset=duplicate_columns, keep='first', inplace=True)
        Table(table_name, MetaData(), autoload_with=engine).drop(engine)
        save_table(db_url, table_name, df)

def drop_table(db_url, table_name):
    engine = create_engine(db_url)
    if table_exists(db_url, table_name):
        Table(table_name, MetaData(), autoload_with=engine).drop(engine)
        print(f"Successfully dropped table {table_name}")

