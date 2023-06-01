import logging
import logging.config
from datetime import datetime


def setup_logging(log_dir: str):
    """Load logging configuration"""

    log_file_name = log_dir + '/' + 'minimal-app-' + \
        datetime.now().strftime("%Y-%m-%d") + '.log'

    loging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'loggers': {
            'root': {
                'level': 'DEBUG',
                'handlers': ['debug_console_handler', 'info_rotating_file_handler'],
            },
            'src': {
                'level': 'DEBUG',
                'propagate': False,
                'handlers': ['info_rotating_file_handler', 'debug_console_handler'],
            },
        },
        'handlers': {
            'debug_console_handler': {
                'level': 'DEBUG',
                'formatter': 'console',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout',
            },
            'info_rotating_file_handler': {
                'level': 'WARN',
                'formatter': 'file',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': log_file_name,
                'mode': 'a',
                'maxBytes': 1048576,
                'backupCount': 10
            }
        },
        'formatters': {
            'console': {
                'format': '%(levelname)s:     %(name)s - %(message)s'
            },
            'file': {
                'format': '%(asctime)s-%(levelname)s-%(name)s-%(process)d::%(module)s|%(lineno)s:: %(message)s'
            },
        },
    }

    logging.config.dictConfig(loging_config)
