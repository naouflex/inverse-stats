import pandas as pd
import traceback
import sqlalchemy.exc as sqla_exc
import io

from sqlalchemy import MetaData, Column, Integer, String, Float, DateTime, Table, select, create_engine, update



def table_exists(db_url, table_name):
    try:
        engine = create_engine(db_url)
        return engine.dialect.has_table(engine.connect(), table_name)
    except:
        return False

def map_dtype(dtype):
    try:
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
        
        if dtype_str in mapping:
            return mapping[dtype_str]
        
        # Check for more general cases
        if "int" in dtype_str:
            return Integer
        elif "float" in dtype_str:
            return Float
        elif "datetime" in dtype_str:
            return DateTime
        elif "str" in dtype_str:
            return String
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.info(traceback.format_exc())
        return 0


def create_table_from_df(engine, table_name, df):
    try:
        meta = MetaData()
        
        columns = []
        for col in df.columns:
            dtype_mapped = map_dtype(df[col].dtype)
            if dtype_mapped == 0:  # This means our mapping failed
                logger.info(f"Mapping failed for column '{col}' with dtype '{df[col].dtype}'.")
                continue  # Skip adding this column
            columns.append(Column(col, dtype_mapped))
        
        if not columns:  # If no columns are mapped correctly
            logger.info(f"No columns were successfully mapped for table '{table_name}'. Check your data.")
            return
        
        table = Table(table_name, meta, *columns)
        table.create(engine)
        logger.info(f"Successfully created table info for {table_name}")
        
    except sqla_exc.SQLAlchemyError as e:
        logger.info(f"SQLAlchemy error occurred: {e}")
        logger.info(traceback.format_exc())
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.info(traceback.format_exc())

def save_table(db_url, table_name, df):
    engine = create_engine(db_url)
    df = pd.DataFrame(df)

    try:
        if not table_exists(db_url, table_name):
            create_table_from_df(engine, table_name, df)

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        raw_conn = engine.raw_connection()

        try:
            # check that table exists and buffer is not empty
            if not table_exists(db_url, table_name):
                logger.info(f"Table {table_name} does not exist")
                return None
            elif len(df) == 0:
                logger.info(f"No rows to save in {table_name}")
                return None
            with raw_conn.cursor() as cur:
                cur.copy_from(buffer, table_name, sep=',', null='')
            raw_conn.commit()
            logger.info(f"Successfully saved {len(df)} rows to {table_name}")
        finally:
            raw_conn.close()

    except sqla_exc.SQLAlchemyError as e:
        logger.info(f"SQLAlchemy error occurred: {e}")
        logger.info(traceback.format_exc())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.info(traceback.format_exc())


def get_table(db_url, table_name):
    engine = create_engine(db_url)

    if not table_exists(db_url, table_name):
        logger.info(f"Table {table_name} does not exist")
        return None
    else:
        meta = MetaData()
        table = Table(table_name, meta, autoload_with=engine)
        stmt = select(table.c)  # Fix is here
        with engine.connect() as conn:
            results = conn.execute(stmt).fetchall()
        df = pd.DataFrame(results, columns=table.columns.keys())
        logger.info(f"Successfully read {len(df)} rows from {table_name}")
        return df

def update_table(db_url, table_name,df):
    engine = create_engine(db_url)
    try:
        if not table_exists(db_url, table_name):
            logger.info(f"Table {table_name} does not exist")
            return None
        df = df[df.columns] 
        existing_df = get_table(db_url,table_name)

        if existing_df is not None:
            existing_df = existing_df[existing_df.columns]
            #filter out rows in df that are already in existing_df
            try:
                df = df[~df.isin(existing_df.to_dict('list')).all(1)]
            except:
                logger.info(f"No new rows to update in {table_name}")
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
                    logger.info(f"Successfully updated {len(df)} rows to {table_name}")
                finally:
                    raw_conn.close()
                    logger.info(f"Successfully closed connection to {table_name}")
        else:
            logger.info(f"Table {table_name} is empty")
            return None 
    except sqla_exc.SQLAlchemyError as e:
        logger.info(f"SQLAlchemy error occurred: {e}")
        logger.info(traceback.format_exc())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.info(traceback.format_exc())

def remove_duplicates(db_url, table_name, duplicate_columns=None, order_column=None):
    engine = create_engine(db_url)

    try:
        if not table_exists(db_url, table_name):
            logger.info(f"Table {table} does not exist")
            return None
        else:
            meta = MetaData()
            table = Table(table_name, meta, autoload_with=engine)
            stmt = select(table.c)  # Fix is here
            with engine.connect() as conn:
                results = conn.execute(stmt).fetchall()

            if duplicate_columns is None:
                duplicate_columns = table.columns.keys()

            if order_column is None:
                order_column = table.columns.keys()[0]

            df = pd.DataFrame(results, columns=table.columns.keys())
            df = df.sort_values(by=[order_column], ascending=False)
            df = df.drop_duplicates(subset=duplicate_columns, keep='first')
            
            # drop table and recreate
            table.drop(engine)
            create_table_from_df(engine, table_name, df)
            save_table(db_url, table_name, df)

    except sqla_exc.SQLAlchemyError as e:
        logger.info(f"SQLAlchemy error occurred: {e}")
        logger.info(traceback.format_exc())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.info(traceback.format_exc())

def drop_table(db_url, table_name):
    engine = create_engine(db_url)
    try:
        if not table_exists(db_url, table_name):
            logger.info(f"Table {table_name} does not exist")
            return None
        else:
            meta = MetaData()
            table = Table(table_name, meta, autoload_with=engine)
            table.drop(engine)
            logger.info(f"Successfully dropped table {table_name}")
    except sqla_exc.SQLAlchemyError as e:
        logger.info(f"SQLAlchemy error occurred: {e}")
        logger.info(traceback.format_exc())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.info(traceback.format_exc())