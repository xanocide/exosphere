#!/usr/bin/env python3
'''
    job.py
    ~~~~~~

    The exosphere job class, this class houses all of the functionality for
    scheduling a job and running a job

    :copyright: © 2018 Dylan Murray
'''

from datetime import datetime, timedelta
import logging

from monthdelta import monthdelta
from croniter import croniter

from exosphere.configs.configs import Configs
from exosphere.lib.decorators import connect
from exosphere.lib import util


CONFIGS = Configs()


class Job():
    '''
        The exosphere job class
    '''

    def __init__(self, job_name=''):

        self.job = self.pull_job_info_from_mongo(job_name)
        self.name = job_name
        self.cron = self.job.get('cron')
        self.trigger = self.job.get('trigger')
        self.dependencies = self.job.get('dependencies', {})
        self.last_report_date = self.job.get('lastReportDate')

    @connect('MONGO')
    def pull_job_info_from_mongo(self, client, job_name):
        '''
            Pulls entire job document from MongoDB to be used by the job class.

        Args:
            MongoDB client (obj)
        Returns:
            Job document (dict)
        '''

        job = list(client.exosphere.jobs.find({'jobName': job_name}))

        if job:
            return job[0]
        else:
            logging.error(
                'No job found with job name: {name}'
                .format(name=self.job_name))
        return {}

    def check_requirements_and_schedule(self):
        '''
            Determines if it is time for a job to be scheduled,
            a job has two types of schedule time (CRON, TRIGGER)
            if a job is cron, check if the cron time is now.
            if a job is trigger check if the job dependencies have
            been met to schedule the job.

        Args:
            None
        Returns:
            Schedules a job to be executed
        '''

        if self.cron:
            if self.cron_job_is_ready_for_scheduling():
                self.schedule(delay=300)
        if self.trigger:
            if self.trigger_job_is_ready_for_scheduling():
                self.schedule()

    def trigger_job_is_ready_for_scheduling(self):
        '''
            Check if a jobs trigger period is has passed since its
            last run date. If the jobs trigger period has passed the job
            is labeled as stale. If the job is stale, check the jobs
            dependencies to make sure they are satisfied. If the jobs
            dependcencies are satisfied it is ready for scheduling.

        Args:
            None
        Returns:
            (bool): True (job is ready for scheduling) | False (job not ready)
        '''

        if (
            self.check_if_job_is_stale(self.name) and
            self.check_job_dependencies()
        ):
            return True
        return False

    def check_job_dependencies(self):
        '''
            Checks the dependencies of a job to make sure they are satisfied
            before publishing. If the depedencies are not met, a job will not
            be scheduled. A job may have two types of dependences:

            1) Another job (checks last report date of job)
            2) Database tables (checks table to make sure current data exists)

        Args:
            None
        Returns
            (bool) True (dependencies met) | False (dependencies not met)
        '''

        job_dependencies = self.dependencies.get('jobs', [])
        database_dependencies = self.dependencies.get('database', [])

        for job in job_dependencies:
            if self.check_if_job_is_stale(job.get('jobName')):
                return False

        if database_dependencies:
            next_report_date = self.get_job_next_report_date()
            for database in database_dependencies:
                if not self.check_if_database_is_ready(
                    database, next_report_date
                ):
                    return False
        return True

    def get_job_next_report_date(self):
        '''
            Returns the next scheduled report date for the job, only if the job
            is trigger. If the job is a cron job this will be ignored.

        Args:
            None
        Returns:
            Report date(str): Next scheduled report date for the job
        '''

        if not self.trigger:
            logging.error(
                'Cannot retrieve next report date, this job is not a '
                'trigger job. Next report date functionality currently'
                'only supported for trigger jobs.')
            return

        if not self.last_report_date:
            logging.error(
                'Cannot retreive the next report date. Last report date for'
                'this job is null and we cannot compare to a null value.')
            return

        last_report_date = datetime.strptime(self.last_report_date, '%Y-%m-%d')

        unit = self.trigger.get('unit')
        value = self.trigger.get('value')

        if not value:
            logging.error((
                'No value supplied in the trigger for job {name} '
                'cannot get next report date without trigger value')
                .format(name=self.name))

        if unit == 'months':
            return (last_report_date + monthdelta(value))
        elif unit in ('days', 'weeks', 'hours', 'minutes', 'seconds'):
            return (last_report_date + timedelta(**{unit: value}))
        else:
            logging.error(
                'Trigger unit for job {job} not supported: {unit}'
                .format(job=self.job.get('name'), unit=unit))
        return

    def check_if_database_is_ready(self, database, value):
        '''
            Check if a table in a database has the required data to run a job.
            If not, job will not publish.

            Args:
                Database (dict): example
                {
                    "schema" : "schema",
                    "field" : "field",
                    "table" : "table",
                    "dbName" : "MONGO"
                }
                value (int | str):  value to search the column in a table for
            Returns:
                (bool): True(value exists in table) | False(value not in table)
        '''

        db = database.get('dbName')
        schema = database.get('schema')
        table = database.get('table')
        column = database.get('column')

        if all([db, schema, table, column, value]):
            if util.value_exists_in_db(db, schema, table, column, value):
                return True
        return False

    def cron_job_is_ready_for_scheduling(self):
        '''
            Check if the jobs cron time is coming up in the next 5 minutes,
            if it is the job is ready to be published with a delay so it will
            run exactly on schedule

        Args:
            None
        Returns:
            (bool): True (job is ready to run) | False (job is not ready)
        '''

        if croniter.is_valid(self.cron):
            if (
                croniter(self.cron, datetime.now()).get_next(datetime) >
                (datetime.now() + timedelta(minutes=5))
            ) and (
                self.check_if_job_is_stale(self.name)
            ):
                return True
        else:
            logging.error(
                'Job cron time is invalid, can not be published: '
                '{job_name}'.format(job_name=self.job.get('name', '~~~')))
        return False

    @connect('MONGO')
    def check_if_job_is_stale(self, job_name):
        '''
            Check if the jobs last report date is stale compared to when it
            is scheduled to next run. If it is stale, it is ready to be run.
            If the report date is not stale, the job is not ready to be run.be

        Args:
            None
        Returns:
            (bool): True (job is ready to run) | False (job is not ready)
        '''

        job = self.pull_job_info_from_mongo(job_name)
        last_report_date = job.get(
            'lastReportDate', datetime(2010, 1, 1, 1, 1))
        cron = job.get('cron')
        trigger = job.get('trigger')

        if cron:
            if croniter.is_valid(cron):
                if (
                    last_report_date <=
                    croniter(cron, datetime.now()).get_next(datetime)
                ):
                    return True
        elif trigger:
            unit = trigger.get('unit')
            value = trigger.get('value')
            if unit == 'months':
                if (
                    last_report_date <=
                    datetime.now() - monthdelta(value)
                ):
                    return True
            elif unit in ('days', 'weeks', 'hours', 'minutes', 'seconds'):
                if (
                    last_report_date <=
                    datetime.now() - timedelta(**{unit: value})
                ):
                    return True
            else:
                logging.error(
                    'Trigger unit for job {job} not supported: {unit}'
                    .format(job=self.job.get('name'), unit=unit))
        return False

    def schedule(self, delay=0):
        '''
            Publish the job to be picked up by a consumer
        '''
        pass
