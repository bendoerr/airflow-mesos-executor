import logging
from Queue import Queue

from airflow.models import Variable, DagBag, DagModel
from airflow.utils.db import provide_session
from airflow.utils.state import State
from mesos.interface import Scheduler
from mesos.interface import mesos_pb2

from airflow_mesos_plugin.executors.mesos_support.config import \
    config_framework_connid, framework_id_for_failover
from airflow_mesos_plugin.executors.mesos_support.filter import \
    AirflowMesosOfferFilter
from airflow_mesos_plugin.executors.mesos_support.resource import default_resources, \
    merge_resources


class AirflowMesosScheduler(Scheduler):
    """
    A Mesos Scheduler for Airflow using the native ProtocolBuffers interface.

    This scheduler does not hold onto any offers but waits to receive offers and
    if has anything to launch will try to launch on one of the received offers
    if possible.

    If offers are unused they are released back to the framework. This helps
    reduce under utilization of mesos resources.

    Speaking of resources, rather than try to naively launch multiple jobs on
    a single offer given enough cpu and mem, this scheduler will only launch a
    single job per received offer. There are many other resource types that
    could come into play and it didn't seem important to devise a generic way
    to track them.

    TODO: Track and relaunch mesos tasks per slave when a slave is lost.
    TODO: More testing in failure scenarios to make sure Airflow tasks are
          always marked in a terminal state and the driver/scheduler responds
          well in adverse conditions.
    TODO: More granular handling for TASK_LOST and other situations when the
          task could rightfully be requeue'd.
    TODO: If the scheduler crashes/aborts/dies while holding a current_task
          it will be stuck in a running state.
    """

    def __init__(self,
                 scheduler_tasks,
                 scheduler_results,
                 scheduler_mesos_task_to_airflow_task
                 ):
        self.scheduler_tasks = scheduler_tasks
        self.scheduler_results = scheduler_results

        # Used to track an airflow task once it's been taken off the
        # scheduler_tasks queue
        self.current_task = None

        self.lost_slaves = Queue()
        self.mesos_task_to_airflow_task = scheduler_mesos_task_to_airflow_task

        self.task_counter = 0


    def disconnected(self, driver):
        """
        Invoked when the scheduler becomes "disconnected" from the master
        (e.g., the master fails and another is taking over).
        """
        Variable.set(config_framework_connid() + "_state", 'disconnected')
        logging.info("MesosScheduler disconnected from mesos")

    def error(self, driver, message):
        """
        Invoked when there is an unrecoverable error in the scheduler or
        scheduler driver. The driver will be aborted BEFORE invoking this
        callback.
        """
        Variable.set(config_framework_connid() + "_state", 'aborted')
        logging.error("MesosScheduler driver aborted, message [%s]", message)

    def executorLost(self, driver, executorId, slaveId, status):
        """
        Invoked when an executor has exited/terminated. Note that any
        tasks running will have TASK_LOST status updates automagically
        generated.
        """
        logging.warning("MesosScheduler executor [%s] on slave [%s] lost, "
                        "status [%s]",
                        str(executorId), str(slaveId), str(status))

    def frameworkMessage(self, driver, executorId, slaveId, message):
        """
        Invoked when an executor sends a message. These messages are best
        effort; do not expect a framework message to be retransmitted in
        any reliable fashion.
        """
        logging.info("MesosScheduler received framework message [%s]", message)

    def offerRescinded(self, driver, offerId):
        """
        Invoked when an offer is no longer valid (e.g., the slave was
        lost or another framework used resources in the offer). If for
        whatever reason an offer is never rescinded (e.g., dropped
        message, failing over framework, etc.), a framework that attempts
        to launch tasks using an invalid offer will receive TASK_LOST
        status updates for those tasks (see Scheduler::resourceOffers).
        """
        logging.debug("MesosScheduler offer [%s] rescinded", str(offerId))

    def registered(self, driver, frameworkId, masterInfo):
        """
        Invoked when the scheduler successfully registers with a Mesos
        master. A unique ID (generated by the master) used for
        distinguishing this framework from others and MasterInfo
        with the ip and port of the current master are provided as arguments.
        """
        Variable.set(config_framework_connid() + "_state", 'registered')
        framework_id_for_failover(frameworkId)
        logging.info(
            "MesosScheduler registered to Mesos (master: [%s:%s]) with "
            "framework id [%s]",
            masterInfo.hostname, masterInfo.port, frameworkId.value)

    def reregistered(self, driver, masterInfo):
        """
        Invoked when the scheduler re-registers with a newly elected Mesos master.
        This is only called when the scheduler has previously been registered.
        MasterInfo containing the updated information about the elected master
        is provided as an argument.
        """
        Variable.set(config_framework_connid() + "_state", 'registered')
        logging.info(
            "MesosScheduler re-registered to Mesos (master: [%s:%s])",
            masterInfo.hostname, masterInfo.port)

    def resourceOffers(self, driver, offers):
        """
        Invoked when resources have been offered to this framework. A
        single offer will only contain resources from a single slave.
        Resources associated with an offer will not be re-offered to
        _this_ framework until either (a) this framework has rejected
        those resources (see SchedulerDriver::launchTasks) or (b) those
        resources have been rescinded (see Scheduler::offerRescinded).
        Note that resources may be concurrently offered to more than one
        framework at a time (depending on the allocator being used). In
        that case, the first framework to launch tasks using those
        resources will be able to use them while the other frameworks
        will have those resources rescinded (or if a framework has
        already launched tasks with those resources then those tasks will
        fail with a TASK_LOST status and a message saying as much).
        """
        try:
            available_offers = list(offers)
            loop = True

            log_mode = logging.info \
                if (not self.scheduler_tasks.empty() or self.current_task) \
                else logging.debug

            log_mode("MesosScheduler.resourceOffers received [%s] offers "
                     "with [%s] tasks queued.",
                     len(available_offers),
                     str(self.scheduler_tasks.qsize()) + \
                     ("+1" if self.current_task else ""))

            while (not self.scheduler_tasks.empty() or self.current_task) \
                    and len(available_offers) > 0 \
                    and loop:

                if not self.current_task:
                    self.current_task = self.scheduler_tasks.get()

                on_offer = self.launch_current_task(available_offers, driver)

                if on_offer:
                    self.current_task = None
                    available_offers.remove(on_offer)
                else:
                    # Non of the offers fulfilled the tasks requirements
                    # wait for more offers
                    logging.info("MesosScheduler.resourceOffers unable find a "
                                 "suitable offer for the current task [%s]."
                                 "Waiting to receive more offers.",
                                 self.current_task[0])
                    loop = False

            # Release any offers we haven't used
            driver.launchTasks([offer.id for offer in available_offers], [])
        except Exception as err:
            logging.error("MesosScheduler.resourceOffers error while running. "
                          "Failing the current task and releasing offers. %s",
                          err)
            driver.launchTasks([offer.id for offer in offers], [])

    def slaveLost(self, driver, slaveId):
        """
        Invoked when a slave has been determined unreachable (e.g.,
        machine failure, network partition). Most frameworks will need to
        reschedule any tasks launched on this slave on a new slave.
        """
        logging.warning("MesosScheduler slave %s lost", str(slaveId))
        self.lost_slaves.put(slaveId)

    def statusUpdate(self, driver, status):
        """
        Invoked when the status of a task has changed (e.g., a slave is
        lost and so the task is lost, a task finishes and an executor
        sends a status update saying so, etc). Note that returning from
        this callback _acknowledges_ receipt of this status update! If
        for whatever reason the scheduler aborts during this callback (or
        the process exits) another status update will be delivered (note,
        however, that this is currently not true if the slave sending the
        status update is lost/fails during that time).
        """
        task_id = status.task_id.value

        if not task_id in self.mesos_task_to_airflow_task:
            # The map may not contain an item if the framework re-registered
            # after a failover. Discard these tasks.
            logging.warn("MesosScheduler.statusUpdate unrecognised task key %s",
                         task_id)
            return

        d = self.mesos_task_to_airflow_task[task_id]
        key = d['key']
        old_status = d['status']

        self.mesos_task_to_airflow_task[task_id] = {
            'key': key,
            'status': status
        }

        if status.state == mesos_pb2.TASK_FINISHED:
            self.mesos_task_to_airflow_task.pop(task_id)
            self.scheduler_results.put((key, State.SUCCESS))

        elif status.state in [mesos_pb2.TASK_FAILED,
                              mesos_pb2.TASK_KILLED,
                              mesos_pb2.TASK_ERROR,
                              mesos_pb2.TASK_LOST,
                              # mesos_pb2.TASK_DROPPED,
                              # mesos_pb2.TASK_GONE
                              ]:
            # TODO check status.reason, see mesos.TaskStatus line 4941
            self.mesos_task_to_airflow_task.pop(task_id)
            self.scheduler_results.put((key, State.FAILED))

        if not old_status or old_status.state != status.state:
            logging.info(
                "MesosScheduler.statusUpdate received status for task [%s] "
                "with mesos state: [%s -> %s], mesos reason: [%s], "
                "and message: [%s]",
                key,
                mesos_pb2.TaskState.Name(
                    old_status.state) if old_status else "",
                mesos_pb2.TaskState.Name(status.state),
                mesos_pb2.TaskStatus.Reason.Name(status.reason),
                str(status.message))

    def reconcile(self, driver):
        if self.mesos_task_to_airflow_task:
            known_status = [task for task
                            in self.mesos_task_to_airflow_task.values()
                            if task['status']]
            logging.debug("MesosScheduler.reconcile asking for reconciliation "
                          "of tasks [%s]", [s['key'] for s in known_status])
            driver.reconcileTasks([s['status'] for s in known_status])

    def launch_current_task(self, offers, driver):
        key, command, task_instance = self.current_task

        needed_resources = self.get_operator_mesos_resources(task_instance)
        logging.info("MesosScheduler.launch_current_task attempting to launch "
                     "the current task [%s] with resource requirements [%s]",
                     str(key), str(needed_resources))

        offer = self.find_offer(offers, needed_resources)

        if offer:
            task_info = self.build_task_info(
                key, command, needed_resources, offer)

            logging.info(
                "MesosScheduler.launch_current_task found suitable offer [%s] "
                "for task [%s] named [%s]",
                offer.id.value, key, task_info.task_id.value)

            driver.launchTasks(offer.id, [task_info])
            self.mesos_task_to_airflow_task[task_info.task_id.value] = {
                'key': key,
                'status': None
            }
            return offer
        else:
            logging.info(
                "MesosScheduler.launch_current_task no suitable offers found "
                "for task [%s]", key)
            return None

    def get_operator_mesos_resources(self, task_instance):
        needed_resources = default_resources()
        operator_task = self.get_operator_task(task_instance)

        if hasattr(operator_task, 'mesos_resources'):
            needed_resources = merge_resources(needed_resources,
                                               operator_task.mesos_resources)

        return needed_resources

    @provide_session
    def get_operator_task(self, task_instance, session=None):
        if hasattr(self.current_task[2], 'operator_task'):
            return self.current_task[2].operator_task

        dag_id = task_instance.dag_id
        task_id = task_instance.task_id

        dag = session.query(DagModel).filter(
            DagModel.dag_id == dag_id
        ).first()

        logging.info("MesosScheduler.get_operator_task reloading DAG file for"
                     "dag [%s] and task [%s]", dag_id, task_id)

        dagbag = DagBag(dag.fileloc)
        bagged_dag = dagbag.dags.get(dag_id)

        if not bagged_dag:
            # DagBag.get_dag does some additional processing for subdags, etc
            bagged_dag = dagbag.get_dag(dag_id)

        for dag_task in bagged_dag.tasks:
            if dag_task.task_id == task_id:
                self.current_task[2].operator_task = dag_task
                return dag_task

        logging.error("MesosScheduler unable to acquire original dag operator "
                      "for dag%s and task %s. Cannot obtain any "
                      "mesos_resources.", dag_id, task_id)

        return object()

    def find_offer(self, offers, needed_resources):
        offer_filter = AirflowMesosOfferFilter(
            [r.to_filter() for r in needed_resources])

        for offer in offers:
            if offer_filter.check(offer):
                return offer

        return None

    def build_task_info(self, key, command, needed_resources, offer):
        tid = self.task_counter
        self.task_counter += 1

        task = mesos_pb2.TaskInfo()
        task.task_id.value = "airflow:%d:%s:%s:%s" \
                             % (tid, key[0], key[1],
                                key[2].strftime('%Y%m%d%H%M%S%f'))
        task.name = ('AirflowTask {'
                     ' MesosTask: %d,'
                     ' DAG: %s,'
                     ' TaskID: %s,'
                     ' Execution: %s }'
                     ) % (tid, key[0], key[1], key[2])
        task.slave_id.value = offer.slave_id.value

        for resource in needed_resources:
            resource.add(task)

        command_info = mesos_pb2.CommandInfo()
        command_info.shell = True
        command_info.value = command
        task.command.MergeFrom(command_info)

        return task
