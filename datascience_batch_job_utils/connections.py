import os
from typing import Optional
from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from datascience_batch_job_utils.utils import is_inside_aws

load_dotenv()


def get_sql_alchemy_engine(db: str,
                           schema: str,
                           ) -> Engine:

    assert os.getenv('snowflake_un') is not None
    assert os.getenv('snowflake_pw') is not None

    return create_engine(
        'snowflake://{user_name}:{password}@{account}/{database}/{schema}?role={role}&warehouse='
        '{warehouse}'.format(
            user_name=os.getenv('snowflake_un'),
            password=os.getenv('snowflake_pw'),
            role=os.getenv('snowflake_role'),
            warehouse=os.getenv('snowflake_wh'),
            account='pattern',
            database=db,
            schema=schema,
        )
    )


def get_snowflake_connector_connection(db: str = 'pattern_db',
                                       schema: Optional[str] = None,
                                       ) -> SnowflakeConnection:
    """
    start a session.

    as long as the connection is not closed, the database connection is kept alive (and temp tables are accessible).
    """

    assert os.getenv('snowflake_un') is not None
    assert os.getenv('snowflake_pw') is not None

    if schema is None:
        if is_inside_aws():
            schema = 'data_science'
        else:
            schema = 'data_science_stage'

    print(f'Using schema="{schema}"')

    conn = snowflake.connector.connect(
        account='pattern',
        user=os.getenv('snowflake_un'),
        password=os.getenv('snowflake_pw'),
        database=db,
        schema=schema,
        role=os.getenv('snowflake_role'),
        warehouse=os.getenv('snowflake_wh'),
    )

    return conn


def snowflake_query_string(query: str,
                           db='PATTERN_DB',
                           schema: Optional[str] = None,
                           ):

    ctx = get_snowflake_connector_connection(db=db, schema=schema)
    try:
        query_results_cursors = ctx.execute_string(query)
        return query_results_cursors
    finally:
        ctx.close()