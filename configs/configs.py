'''
    Holds all of the configurations for the project
    Call the class inside of new scripts and instantly
    import all known configurations
'''

import logging
import sys
import os


def get_my_file_name():
    '''
        Finds the file name of a script calling the function

    Args:
        None
    Returns:
        File name (str)
    '''
    return os.path.basename(sys.argv[0])


class Configs():
    '''
        Configs class houses the basic configurations to be used
        in a bin script such as a function to return a logger
        preconfigured to use the callers file name
    '''

    def get_logging(self):
        '''
            Gets a logging instance prepared for a caller by using
            the callers file name in the loggers configurations

        Args:
            None
        Returns:
            An instance of logging (class)
        '''

        logging_file = (get_my_file_name() + '.log')
        log_format = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'

        return logging.basicConfig(
            filename=logging_file,
            format=log_format
        )

    MONGO_SERVER_IP = '45.33.80.139'
    MONGO_CLIENT_LOCATION = 'mongodb://localhost:27017/'
