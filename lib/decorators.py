'''
decorators.py
~~~~~~~~~~~~~

General decorators that are used throughout the exosphere
project.

:copyright: © 2018 Dylan Murray
'''

import functools
import logging
from datetime import datetime

from exosphere.configs.configs import Configs


CONFIGS = Configs()


def execute(f):
    '''
    Decorator to log script run times

    Args:
        f(function): Function you wish to wrap
    Returns:
        Decorated function that logs how much time a script takes
    '''

    def execute_function(*args, **kwargs):

        start_time = datetime.now()
        logging.info('-- Starting script -- {time}'.format(time=start_time))

        try:
            f(*args, **kwargs)
        except Exception:
            # Send error to sentry in the future?
            # or send error to custom UI feature?
            raise

        end_time = datetime.now()
        logging.info(
            '-- Script completed succesfully! -- Run Time: {time}'
            .format(time=(end_time - start_time)))

    return execute_function


def connect(db):
    '''
    Connects to a database and passes a cursor to
    a function to be used

    Args:
        db(str): Database you wish to connect to
    Returns:
        Function with db cursor passed as an argument
    '''

    def decorated_function(f):

        @functools.wraps(f)
        def wrap_connection(*args, **kwargs):

            if db == 'MONGO':

                from pymongo import MongoClient

                client = MongoClient(CONFIGS.MONGO_CLIENT_LOCATION)

                return f(client, *args, **kwargs)

            else:
                raise Exception('This database currently not supported! ' + db)
        return wrap_connection
    return decorated_function
