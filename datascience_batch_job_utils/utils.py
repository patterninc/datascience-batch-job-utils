from typing_extensions import ParamSpec
from functools import wraps
from typing import Union, Optional, Tuple, Literal, Callable, List
import datetime
import sys
import time
from pathlib import Path
import pandas as pd
import logging
from logging import Logger
import os
import traceback
import psutil
import pytz
import boto3
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy.engine import Engine
import pybrake

from datascience_batch_job_utils.helpers import RecordCollector
from datascience_batch_job_utils.exceptions import EmptyQueryResults


def is_inside_aws():
    load_dotenv()
    return True if os.getenv('INSIDE_AWS', 'True').lower() == 'true' else False


def push_to_snowflake(engine: Engine,
                      table_name: str,
                      df: pd.DataFrame,
                      logger: Logger,
                      if_exists: Literal['append', 'fail', 'replace'] = 'append',
                      print_df_info: bool = False,
                      add_created_date: bool = False,
                      is_scheduled: Optional[bool] = None,
                      ) -> None:

    if df.empty:
        print('Not writing to snowflake. Passed df is empty')
        return

    # add datetime column
    if add_created_date:
        df['created_date'] = datetime.datetime.now().astimezone(pytz.utc)

    # add column
    if is_scheduled is not None:
        df['is_scheduled'] = is_scheduled

    table_name = table_name.lower()  # avoid errors pushing to snowflake

    # show memory usage of dataframe, and other useful stats
    if print_df_info:
        print(df.info())

    if logger is not None:
        if if_exists == 'append':
            logger.info(f'Appending {len(df):,} rows to {table_name.upper()}.')
        elif if_exists == 'replace':
            logger.info(f'Replacing {table_name.upper()} with {len(df):,} rows.')
        else:
            raise AttributeError('Invalid arg to if_exists.')

    df.columns = df.columns.str.upper()

    df.to_sql(name=table_name,
              con=engine,
              if_exists=if_exists,
              index=False,
              method=pd_writer,
              chunksize=16384,  # otherwise, error if too much data is pushed
              )

    df.columns = df.columns.str.lower()


def get_logger(name: Optional[str] = None,
               level: int = logging.INFO,
               log_file_path: Optional[Path] = None,
               use_airbrake: bool = False,
               pybrake_env_name: str = 'production',
               pybrake_log_level: int = logging.CRITICAL,
               collector_log_level: int = logging.WARNING,
               ) -> Union[Tuple[Logger, Path], Logger]:

    if name is None:
        name = Path(__file__).parent.parent.name  # should be the name of project dir

    # init logger
    logger = logging.getLogger(name)  # Get the logger from logging package
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s',
                                  "%Y-%m-%d %H:%M:%S")  # Formatting for the logs

    # Set up the stream handler for the logger
    stream_handler = logging.StreamHandler(sys.stdout)  # Create a stream handler object
    stream_handler.setFormatter(formatter)  # Set the formatting for the handler
    logger.addHandler(stream_handler)  # Add the stream handler for the logger

    logger.setLevel(level)  # level above which a message is passed to all handlers

    # Set up the file handler for the logger
    if log_file_path:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Set up PyBrake handler for the logger
    if use_airbrake:
        airbrake_project_id = os.getenv('airbrake_project_id')
        airbrake_key = os.getenv('airbrake_key')
        if airbrake_key is None or airbrake_project_id is None:
            raise AttributeError('Did not find airbrake_project_id and/or airbrake_key in environment variables.')

        notifier = pybrake.Notifier(project_id=airbrake_project_id,
                                    project_key=airbrake_key,
                                    environment=pybrake_env_name,
                                    )
        airbrake_handler = pybrake.LoggingHandler(notifier=notifier,
                                                  level=pybrake_log_level)
        logger.addHandler(airbrake_handler)

    # set up a handler that collects messages only above some log level.
    # note: the collector is used to decide if the log should be published.
    collector = RecordCollector()
    collector.setLevel(collector_log_level)
    logger.addHandler(collector)

    return logger


def is_asin_valid(asin: str,
                  ) -> bool:

    if not isinstance(asin, str):
        return False

    if len(asin) != 10:
        return False

    return True


def log_completion(logger: Logger,
                   fn: Callable,
                   time_taken: Union[float, None],
                   print_memory_info: bool = True,
                   ):

    if print_memory_info:
        mbs = psutil.Process().memory_info().rss / (1024 * 1024)
        logger.info(f'Current process is using {mbs:.2f} MBs of resident memory')

    if time_taken is None:
        logger.info(f'{fn.__name__} completed without runtime measurement.')
    else:
        logger.info(f'{fn.__name__} took {round(time_taken, 1)} seconds')


def log_failure(logger: Logger,
                fn: Callable,
                ex: Exception,
                is_critical: bool = False,
                print_traceback: bool = False,
                ):

    mbs = psutil.Process().memory_info().rss / (1024 * 1024)
    logger.info(f'Current process is using {mbs:.2f} MBs of resident memory')

    if is_critical:
        logger.critical(f'{fn.__name__} failed with {type(ex).__name__}: {ex}')
    else:
        logger.warning(f'{fn.__name__} failed with {type(ex).__name__}: {ex}')

    if print_traceback:
        traceback.print_exc()


def publish_log_file(log_file_path: Path,
                     logger: Logger,
                     project_name: str,
                     subject: Optional[str] = None,
                     region_name: str = 'us-west-2',
                     owner: str = 'Philip Huebner',
                     profile_name: str = 'dev',
                     account_id: str = '840725391265',  # development account
                     ) -> None:
    """
    send the log file via AWS SNS.
    note: you must manually create a topic in AWS SNS in the management console for this to work.
    """

    # decide if to publish
    for handler in logger.handlers:
        if handler.name == 'collector':
            handler: RecordCollector
            # if no records in the collector, do not publish
            if not handler.records:
                return

    if is_inside_aws():
        client = boto3.client('sns')
    else:

        boto3.setup_default_session(profile_name=profile_name)
        client = boto3.client('sns', region_name=region_name)
    if log_file_path.exists() and is_inside_aws():
        topic_arn = f'arn:aws:sns:{region_name}:{account_id}:{project_name}'

        client.publish(
            TopicArn=topic_arn,
            Message=log_file_path.read_text(),
            Subject=subject or f'{project_name}-log',
            MessageAttributes={
                'Owner': {
                    'DataType': 'String',
                    'StringValue': owner,
                }
            },
        )
        print(f'Published log file to topic {topic_arn}')
    else:
        print(f'Path to log file {log_file_path} does not exist. Cannot publish.')


def to_sql_safe_list(iterable: List[Union[str, int]],
                     ) -> str:
    """
    format list of values for SQL queries
    """

    if not iterable:
        raise ValueError(f'{to_sql_safe_list.__name__} encountered empty iterable.')

    if isinstance(iterable[0], int):
        return "(" + ", ".join([str(i) for i in iterable]) + ")"
    elif isinstance(iterable[0], str):
        return "('" + "', '".join([to_sql_safe_string(i) for i in iterable]) + "')"
    else:
        raise RuntimeError(f'{to_sql_safe_list.__name__} does not support {iterable}.')


def to_sql_safe_string(brand_name: str) -> str:
    """
    Escape apostrophes by doubling them up in the name.
    E.g. 'Crafter's Companion' --> 'Crafter''s Companion'
    """
    brand_name = brand_name.replace('\'', '\'\'')
    return brand_name


QueryFnInp = ParamSpec('QueryFnInp')


def raise_exception_if_empty(fn: Callable[QueryFnInp, pd.DataFrame],
                             verbose: bool = True,
                             ) -> [QueryFnInp, pd.DataFrame]:
    @wraps(fn)  # necessary so that __name__ returns the name of the wrapped function instead of the decorator
    def wrapper(*args: QueryFnInp.args,
                **kwargs: QueryFnInp.kwargs,
                ) -> pd.DataFrame:

        start = time.time()

        if verbose:
            print(f'Started SQL query: {fn.__name__}')

        df: pd.DataFrame = fn(*args, **kwargs)

        if verbose:
            print(f'Completed SQL query: {fn.__name__} in {time.time() - start} seconds')

        if df.empty:
            raise EmptyQueryResults(fn_name=fn.__name__)
        else:
            return df

    return wrapper
