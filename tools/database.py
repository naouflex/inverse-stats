import pandas as pd
import traceback
from sqlalchemy import create_engine, text,MetaData, Table, Column, String, Integer,select,exc as sqla_exc
import io



def table_exists(engine, table_name):
    return engine.dialect.has_table(engine.connect(), table_name)

def create_table_from_df(engine, table_name,df):
    try:
        meta = MetaData()
        columns = [Column(col, String) if df[col].dtype == 'O' else Column(col, Integer) for col in df.columns]
        table = Table(table_name, meta, *columns)
        table.create(engine)
        print(f"Successfully created table {table_name}")
    except sqla_exc.SQLAlchemyError as e:
        print(f"SQLAlchemy error occurred: {e}")
        print(traceback.format_exc())
    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())

def save_table(db_url, table_name, df):
    engine = create_engine(db_url)
    df = pd.DataFrame(df)  # make sure df is a dataframe

    try:
        if not table_exists(engine, table_name):
            create_table_from_df(engine, table_name, df)

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        raw_conn = engine.raw_connection()

        try:
            with raw_conn.cursor() as cur:
                cur.copy_from(buffer, table_name, sep=',', null='')
            raw_conn.commit()
            print(f"Successfully saved {len(df)} rows to {table_name}")
        finally:
            raw_conn.close()
            print(f"Successfully closed connection to {table_name}")

    except sqla_exc.SQLAlchemyError as e:
        print(f"SQLAlchemy error occurred: {e}")
        print(traceback.format_exc())
    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())


def get_table(db_url, table_name):
    engine = create_engine(db_url)

    if not table_exists(engine, table_name):
        print(f"Table {table_name} does not exist")
        return None
    else:
        meta = MetaData()
        table = Table(table_name, meta, autoload_with=engine)
        stmt = select(table.c)  # Fix is here
        with engine.connect() as conn:
            results = conn.execute(stmt).fetchall()
        df = pd.DataFrame(results, columns=table.columns.keys())
        print(f"Successfully read {len(df)} rows from {table_name}")
        return df

def update_table(db_url, table_name,df):
    engine = create_engine(db_url)
    try:
        if not table_exists(engine, table_name):
            print(f"Table {table_name} does not exist")
            return None
        df = df[df.columns] 
        existing_df = get_table(db_url,table_name)

        if existing_df is not None:
            existing_df = existing_df[existing_df.columns]
            df = df[~df.isin(existing_df)].dropna()
            if len(df) == 0:
                print(f"No new rows to update in {table_name}")
                return None
            else:
                buffer = io.StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)
                raw_conn = engine.raw_connection()

                try:
                    with raw_conn.cursor() as cur:
                        cur.copy_from(buffer, table_name, sep=',', null='')
                    raw_conn.commit()
                    print(f"Successfully updated {len(df)} rows to {table_name}")
                finally:
                    raw_conn.close()
                    print(f"Successfully closed connection to {table_name}")
        else:
            print(f"Table {table_name} is empty")
            return None 
    except sqla_exc.SQLAlchemyError as e:
        print(f"SQLAlchemy error occurred: {e}")
        print(traceback.format_exc())
    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())