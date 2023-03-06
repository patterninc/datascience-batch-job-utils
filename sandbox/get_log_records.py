import logging

from datascience_batch_job_utils.helpers import RecordCollector



# create a logger and add the RecordCollector handler
logger = logging.getLogger('my_logger')
collector = RecordCollector()
collector.setLevel(logging.ERROR)
logger.addHandler(collector)

# generate some log messages
logger.debug('This is a debug message')
logger.info('This is an info message')
logger.warning('This is a warning message')
logger.error('This is an error')
logger.critical('This is a critical error')

# retrieve all the LogRecord objects created by the logger
log_records = collector.records

print(log_records)

for handler in logger.handlers:
    print(handler)
    print(handler.name)
    print(dir(handler))