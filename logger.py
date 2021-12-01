import logging
import logging.handlers


class Logger():

    def __init__(self , filename):
        self.filename = filename

    def logger(self):
        handler=logging.handlers.RotatingFileHandler(self.filename, maxBytes=10000000, backupCount=5)
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '[%(asctime)s] - %(filename)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        log.addHandler(handler)
        return log