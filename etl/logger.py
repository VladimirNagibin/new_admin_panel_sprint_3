import logging
import sys
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('load_data_elastic')
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    ('%(asctime)s - %(name)s - %(funcName)s - %(lineno)s - %(levelname)s - '
     '%(message)s')
)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
file_handler = RotatingFileHandler(
    'logs/load_data_elastic.log',
    maxBytes=500000,
    backupCount=3,
    encoding='utf-8'
)
file_handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(file_handler)
