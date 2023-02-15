import logging
import time
import pandas as pd

from sqlalchemy.sql import text as sa_text
import sqlalchemy.schema as sch
from src import engine

def reflect_all_schemas():
    metadata_obj =sch. MetaData()
    metadata_obj.reflect(bind=engine)
    schemas = {}
    for key in metadata_obj.tables.keys():
        schemas[key] = {column.name : column.type for column in metadata_obj.tables[key].c}

    return schemas 

def reflect_table_to_schema(tableName: str):
    metadata_obj =sch. MetaData()
    metadata_obj.reflect(bind=engine)
    return {column.name : column.type for column in metadata_obj.tables[tableName].c}

def upsert_df(df: pd.DataFrame, schema: str, table: str, key_columns: list, dtype: dict = None):
    if dtype is None:
        schema_dtype = reflect_table_to_schema(table)
    else:
        schema_dtype = dtype

    retry_task = True
    retry_count = 0
    time_sleep = 5

    value_columns = [x for x in list(df.columns) if x not in key_columns]

    while retry_task and retry_count < 5:

        try:
            df.to_sql(name=table, con=engine, schema='temp', if_exists='replace', index=False, chunksize=50000, dtype=schema_dtype)
            retry_task = False
        except:
            logging.warning(f"Retry after {time_sleep} seconds")
            retry_count = retry_count + 1
            time.sleep(time_sleep)

    # build merge statement
    sql_condition = ' AND '.join(['t.' + item + ' = s.' + item for item in key_columns])
    sql_update = 'UPDATE SET ' + ', '.join([item + ' = s.' + item for item in value_columns])
    sql_insert = 'INSERT (' + ', '.join(key_columns + value_columns) + ') VALUES (' + ', '.join(['s.' + item for item in key_columns + value_columns]) + ')'
    sql_merge = f'''MERGE INTO {schema}.{table} AS t 
                    USING temp.{table} AS s ON {sql_condition}
                    WHEN MATCHED THEN
                        {sql_update}
                    WHEN NOT MATCHED THEN
                        {sql_insert}
                    -- WHEN NOT MATCHED THEN
                    -- DELETE
                    ;
                    '''

    # execute merge
    engine.execute(sa_text(sql_merge).execution_options(autocommit=True))

    # delete temp table
    sql_delete = f'DROP TABLE temp.{table};'
    engine.execute(sa_text(sql_delete).execution_options(autocommit=True))
