#!/usr/bin/env python3
'''
'''
import time
import math
import requests
import socket
from datetime import datetime

from exosphere.configs.configs import Configs
from exosphere.lib.decorators import connect


CONFIGS = Configs()
logging = CONFIGS.get_logging()


class Scheduler():
    '''
        Exosphere scheduler class, used for scheduling exosphere jobs.
        The scheduler is designed to be highly available and scalable
        to allow multiple schedulers to be running at once. Incase of failure
        a new scheduler will take the role of primary if multiple instances
        of the scheduler is running.
    '''

    def __init__(self):

        self.hostname = socket.gethostbyname(socket.gethostname())
        self.mongo_client = self.open_mongo_connection()

    def schedule(self):
        '''
            Starts the scheduler
        '''

        self.scheduler_score = self.generate_scheduler_score()
        self.create_scheduler_information()

        while True:
            if not self.check_for_a_primary_schedulure():
                self.set_scheduler_to_primary()
                self.ensure_there_is_only_one_primary_scheduler()
                if self.am_i_still_primary_scheduler():
                    self.start_kicking_off_jobs()
                    break
            elif self.should_i_be_primary_scheduler():
                self.set_scheduler_to_primary()
                self.ensure_there_is_only_one_primary_scheduler()
                if self.am_i_still_primary_scheduler():
                    self.start_kicking_off_jobs()
                    break

            # Sleep for 5 minutes before we check if we should be the primary
            # scheduler again
            time.sleep(300)

    @connect('MONGO')
    def open_mongo_connection(client, self):
        '''
            Opens a mongo client connection upon class initialization so we
            do not have to open and close a many connections as the tool runs

        Args:
            None
        Returns:
            Mongo Client (cursor)
        '''
        return client

    def create_scheduler_information(self):
        '''
            Writes the initial scheduler information when the scheduler
            begins to run to mongo. Allows us to visually see which shcedulures
            are running and where, as well as which schedulers are primary
            or secondary.

        Args:
            None
        Returns:
            Inserts scheduler information to MongoDB
        '''

        try:
            self.mongo_client.exosphere.schedulers.insert_one({
                'hostname': self.hostname,
                'startedAt': datetime.utcnow(),
                'lastCheckedIn': datetime.utcnow(),
                'score': self.scheduler_score,
                'primary': False
            })
        except Exception as err:
            logging.error(
                'Failed writing scheduler information to MongoDB: {err}'
                .format(err=err))
            raise

    def get_request_speed(self):
        '''
            Ping the database server and return the ping speed
            used for calculating scheduler score

        Args:
            None
        Returns:
            Ping Speed (float)
        '''

        return requests.get(
            'http://' + CONFIGS.MONGO_SERVER_IP).elapsed.total_seconds()

    def generate_scheduler_score(self):
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
            score = sum([
                self.get_request_speed(),
                self.get_request_speed(),
                self.get_request_speed()
            ])
        except Exception as err:
            logging.error(
                'Failed pinging the MongoDB server, we have a problem: {err}'
                .format(err=err))
            raise

        return (score * 100)

    def should_i_be_primary_scheduler(self):
        '''
            Pull active schedulers from MongoDB to determine
            if this scheduler instance should be the primary
            scheduler or not

        Args:
            client(pymongo): Mongo client
            score(float): Score of current scheduler
        Returns:
            (bool): True if I have the best score, False if not
        '''

        try:
            schedulers = list(self.mongo_client.exosphere.schedulers.find({}))
        except Exception as err:
            logging.error(
                'Failed retrieving scheduler information from MongoDB: {err}'
                .format(err=err))
            return False

        if schedulers:
            for scheduler in schedulers:
                if scheduler.get('primary', False):
                    return False
                elif scheduler.get('score', math.inf) < self.scheduler_score:
                    return False
        return True

    def check_for_a_primary_schedulure(self):
        '''
            Checks if there is currently an active primary scheduler or not

        Args:
            None
        Returns:
            (bool): True if an active schedulure exists, else False
        '''

        try:
            primary_scheduler = list(
                self.mongo_client.exosphere.schedulers.find({'primary': True}))
        except Exception as err:
            logging.error(
                'Failed attempt to pull primary schedulers from MongoDB: {err}'
                .format(err=err))
            return True

        if primary_scheduler:
            return True
        return False

    def set_scheduler_to_primary(self):
        '''
            Sets the scheduler to primary status if we determine we should be
            the primary

        Args:
            None
        Returns:
            Sets the current scheduler to the primary in MongoDB
        '''

        try:
            self.mongo_client.exosphere.schedulers.update(
                {'hostname': self.hostname},
                {'$set': {'primary': True}}
            )
        except Exception as err:
            logging.error(
                'Failed setting scheduler to primary status: {err}'
                .format(err=err))
            raise

    def ensure_there_is_only_one_primary_scheduler(self):
        '''
            Check MongoDB to ensure there is not more than one primary
            scheduler. If more than one primary scheduler exists, set only the
            scheduler with the highest score as the primary.

        Args:
            None
        Returns:
            Updates primary scheduler status in MongoDB if necessary
        '''

        primary_schedulers = list(
            self.mongo_client.exosphere.schedulers.find({'primary': True}))

        if len(primary_schedulers) > 1:

            self.mongo_client.exosphere.schedulers.update_many(
                {'$set': {'primary': False}})

            best_score = 0
            new_primary = None
            for scheduler in primary_schedulers:
                if scheduler.get('score', math.inf) < best_score:
                    new_primary = scheduler.get('hostname')

            self.mongo_client.exosphere.schedulers.update(
                {'hostname': new_primary},
                {'$set': {'primary': True}}
            )

    def am_i_still_primary_scheduler(self):
        '''
            A check to ensure that this shceduler is still the primary
            scheduler before we begin to schedule jobs to avoid duplicate
            job runs and queue complications

        Args:
            None
        Returns:
            (bool): True i'm secondary, False i'm not
        '''

        scheduler = list(
            self.mongo_client.exosphere.schedulers.find(
                {'hostname': self.hostname}))

        if scheduler:
            status = scheduler[0].get('primary', False)
            if status:
                return True
            else:
                return False
        return False

    def start_kicking_off_jobs(self):
        '''
        '''
        pass
