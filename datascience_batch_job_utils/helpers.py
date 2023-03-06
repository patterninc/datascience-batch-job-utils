import logging


class RecordCollector(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []
        self.name = 'collector'

    def emit(self, record):
        self.records.append(record)