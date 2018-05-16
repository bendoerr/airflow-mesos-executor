import logging
from Queue import Queue
from datetime import time

import mesos.native
from airflow.executors import BaseExecutor
from airflow.executors.base_executor import PARALLELISM
from mesos.interface import mesos_pb2
from datetime import datetime, timedelta

from airflow_mesos_plugin.executors.mesos_support.config import config_master_url, \
    config_framework_name, config_framework_checkpoint, \
    config_framework_failover_timeout, framework_id_for_failover, \
    config_framework_authenticate
from airflow_mesos_plugin.executors.mesos_support.scheduler import \
    AirflowMesosScheduler


class MesosExecutor(BaseExecutor):
    def __init__(self,
                 parallelism=PARALLELISM):
        super(MesosExecutor, self).__init__(parallelism)

        self.scheduler_tasks = Queue()
        self.scheduler_results = Queue()
        self.scheduler_mesos_task_to_airflow_task = {}

        self.queued_task_instances = {}

        self.master = None
        self.framework = None
        self.credential = None
        self.scheduler = None
        self.driver = None

        self.suppressed = False

        self.reconcile_interval = timedelta(minutes=5)
        self.reconcile_last = datetime.now()

    def start(self):
        self.master = config_master_url()
        self.build_framework()
        self.build_credential()
        self.build_scheduler()
        self.build_driver()
        self.driver.start()

    def queue_command(self, task_instance, command, priority=1, queue=None):
        """
        Override queue_command to capture the task_instance which is needed to
        lookup the DAG to acquire any specific mesos resources it needs
        """
        super(MesosExecutor, self).queue_command(
            task_instance, command, priority, queue)

        key = task_instance.key
        if key not in self.queued_task_instances and key not in self.running:
            self.queued_task_instances[key] = task_instance

    def sync(self):
        """
        Sync will get called periodically by the heartbeat method.
        """
        self.sync_mesos()
        self.sync_results()

    def execute_async(self, key, command, queue=None):
        """
        Called by the heartbeat method to submit a job to the MesosScheduler
        """
        task_instance = self.queued_task_instances.pop(key)
        self.scheduler_tasks.put((key, command, task_instance))
        self.mesos_suppress_revive()

    def end(self):
        """
        The pydoc in BaseExecutor.end seems to imply this is called when a
        caller has submitted a job and wants to wait for it to complete, however
        in practice it seems that this is called when the caller would like to
        shutdown the executor.
        """
        logging.info("MesosExecutor has been asked to end")

        # Wait for running tasks to all be complete
        while self.running or self.queued_tasks:
            self.sync()
            logging.info("Waiting for running or queued tasks to complete")
            time.sleep(10)

        self.sync()
        self.driver.stop()

    def terminate(self):
        self.driver.abort()

    def sync_mesos(self):
        # ScheduleDriver.start appears to be idempotent and will just return the
        # current status rather than try to start the driver back up again.
        driver_status = self.driver.start()
        if driver_status != mesos_pb2.DRIVER_RUNNING:
            logging.info("SchedulerDriver has status %s",
                         mesos_pb2.Status.Name(driver_status))

        if driver_status == mesos_pb2.DRIVER_ABORTED:
            logging.info("Trying to restart SchedulerDriver")
            self.start()
            # Force reconciliation after restart
            self.scheduler.reconcile(self.driver)

        # Ask the Scheduler to reconcile any mesos tasks that is aware of with
        # the mesos master.
        if self.reconcile_last + self.reconcile_interval < datetime.now():
            self.scheduler.reconcile(self.driver)
            self.reconcile_last = datetime.now()

        # Update suppress/revive offers status
        self.mesos_suppress_revive()

    def sync_results(self):
        # Grab results from the scheduler and let the Airflow framework know
        # how they ended
        while not self.scheduler_results.empty():
            result = self.scheduler_results.get()
            self.change_state(*result)

    def mesos_suppress_revive(self):
        if self.scheduler_tasks.empty() and not self.suppressed:
            logging.info("Suppressing mesos offers.")
            self.suppressed = True
            self.driver.suppressOffers()

        if not self.scheduler_tasks.empty() and self.suppressed:
            logging.info("Reviving mesos offers.")
            self.suppressed = False
            self.driver.reviveOffers()


    def build_framework(self):
        framework = mesos_pb2.FrameworkInfo()
        framework.user = ''
        framework.name = config_framework_name()

        if config_framework_checkpoint():
            framework.checkpoint = True

            if config_framework_failover_timeout():
                framework.failover_timeout = config_framework_failover_timeout()

                previous_framework_id = framework_id_for_failover()
                if previous_framework_id:
                    framework.id.value = previous_framework_id

        else:
            framework.checkpoint = False

        self.framework = framework

    def build_credential(self):
        assert self.framework, "build_framework must be called first"

        auth = config_framework_authenticate()
        if auth:
            principal, secret = auth
            credential = mesos_pb2.Credential()
            credential.principal = principal
            credential.secret = secret

            self.credential = credential
            self.framework.principal = principal
        else:
            self.framework.principal = ''

    def build_scheduler(self):
        self.scheduler = \
            AirflowMesosScheduler(self.scheduler_tasks,
                                  self.scheduler_results,
                                  self.scheduler_mesos_task_to_airflow_task
                                  )

    def build_driver(self):
        assert self.framework, "build_framework must be called first"
        assert self.scheduler, "build_scheduler must be called first"
        assert self.master, "master must be set first"

        if self.credential:
            self.driver = mesos.native.MesosSchedulerDriver(
                self.scheduler, self.framework, self.master, 1, self.credential)
        else:
            self.driver = mesos.native.MesosSchedulerDriver(
                self.scheduler, self.framework, self.master, 1)
