#!/usr/bin/env python3
'''
'''
import requests
import math
import socket

from exosphere.configs.configs import Configs
from exosphere.lib.decorators import connect


CONFIGS = Configs()
logging = CONFIGS.get_logging()


@connect('MONGO')
def update_scheduler_information(client):
    '''
        Updates information about this scheduler to mongo

    Args:
        client(pymongo): Mongo client
    Returns:
        Inserts scheduler information to mongodb
    '''

    score = generate_scheduler_score()
    i_am_primary = should_i_be_primary_scheduler(score)
    hostname = socket.gethostname()

    try:
        client.exosphere.schedulers.update(
            {'_id': hostname},
            {
                '_id': hostname,
                'hostname': hostname,
                'score': score,
                'primary': i_am_primary
            },
            upsert=True
        )
    except Exception as err:
        logging.error(
            'Failed updating scheduler at this time: {err}'
            .format(err=err))


def generate_scheduler_score():
    '''
        Generates a score based on the ping to the database server
        to determine whether this scheduler should be the primary
        or secondary scheduler

    Args:
        client(pymongo): Mongo client
    Returns:
        ping_speed(float): Time it took to ping the db server
    '''

    try:
        ping_speed = requests.get(
            'http://' + CONFIGS.MONGO_SERVER_IP).elapsed.total_seconds()
    except Exception as err:
        logging.error(
            'Failed pinging the server, we have a problem: {err}'
            .format(err=err))
        ping_speed = 0

    return (ping_speed * 1000000)


@connect('MONGO')
def should_i_be_primary_scheduler(client, score):
    '''
        Pull active primary schedulers from db to determine
        if i should be the new primary scheduler or not

    Args:
        client(pymongo): Mongo client
        score(float): Score of current scheduler
    Returns:
        (bool): True if I have the best score, False if not
    '''

    schedulers = list(client.exosphere.schedulers.find({}))

    if schedulers:
        for scheduler in schedulers:
            if scheduler.get('score', math.inf) < score:
                return False
    return True
