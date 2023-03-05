import os
from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.connector import SnowflakeConnection

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


def get_snowflake_connector_connection(schema: str
                                       ) -> SnowflakeConnection:
    """
    start a session.

    as long as the connection is not closed, the database connection is kept alive (and temp tables are accessible).
    """

    assert os.getenv('snowflake_un') is not None
    assert os.getenv('snowflake_pw') is not None

    conn = snowflake.connector.connect(
        account='pattern',
        user=os.getenv('snowflake_un'),
        password=os.getenv('snowflake_pw'),
        database='PATTERN_DB',
        schema=schema,
        role=os.getenv('snowflake_role'),
        warehouse=os.getenv('snowflake_wh'),
    )

    return conn
