from airflow.plugins_manager import AirflowPlugin

from airflow_mesos_plugin.executors import MesosExecutor

__all__ = [
    'MesosExecutor',
]

class AirflowMesosPlugin(AirflowPlugin):
    name = "airflow_mesos_plugin"
    executors = [MesosExecutor]

