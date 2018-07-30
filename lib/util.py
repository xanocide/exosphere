#!/usr/bin/env python3
'''
    util.py
    ~~~~~~~

    This file hosues general utility functions that will be used throughout
    the exosphere project.

    :copyright: © 2018 Dylan Murray
'''

import logging

from exosphere.configs.configs import Configs
from exosphere.lib.decorators import connect


CONFIGS = Configs()


def value_exists_in_db(db, schema, table, column, value):
    '''
        Checks if a value exists in a specific table in a database. Uses the
        connect decorator so make sure the database you are trying to
        check has connection information in the decorator.

    Args:
        db(str): database name
        schema(str): schema in a database
        table(str): table in a database
        column(str): column in a database table
        value(str,int): value to search for in a database table

    Returns:
        (bool): True (value exists in the table) | False (value does not exist)
    '''

    if not all([db, schema, column, value]):
        logging.error('Missing required arguments to check for a value.')
        return

    if type(value) not in (str, int, float):
        logging.error('Value type not supported by this util.')
        return

    @connect(db)
    def check_sql(db_cur, *args, **kwargs):
        '''
            Checking if a value exists in a sql database
        '''

        if isinstance(value, str):
            query_value = """'{value}'""".format(value=value)
        elif type(value) in (int, float):
            query_value = value
        else:
            logging.error('Value input is not supported in this util.')
            return

        exists_query = """
        SELECT *
        FROM {schema}.{table}
        WHERE {column} = {value}
        LIMIT 1
        """.format(schema=schema, table=table, field=column, value=query_value)

        logging.info(
            'Checking if a value exists in SQL: {query}'
            .format(query=exists_query))

        db_cur.execute(exists_query)
        results = db_cur.fetchall()

        if results:
            return True
        return False

    @connect(db)
    def check_mongo(client, *args, **kwargs):
        '''
            Check if a value exists in a mongo collection
        '''

        results = list(client[schema][table].find({column: value}).limit(1))

        logging.info(
            'Checking MongoDB [{schema}][{collection}] for {value}'
            .format(schema=schema, collection=table, value=value))

        if results:
            return True
        return False

    if db.lower().endswith('sql'):
        return check_sql()
    elif db.lower() == 'mongo':
        return check_mongo()
    return False
