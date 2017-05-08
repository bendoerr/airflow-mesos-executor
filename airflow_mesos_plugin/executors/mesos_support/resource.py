import json

from airflow import configuration
from mesos.interface import mesos_pb2

from airflow_mesos_plugin.executors.mesos_support.filter import \
    AirflowMesosResourceFilter, AirflowMesosHostnameFilter


class MesosResource(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return str(self.__dict__)

    def add(self, mesos_task):
        raise NotImplementedError()

    def to_filter(self):
        raise NotImplementedError()


class MesosScalarResource(MesosResource):
    def add(self, mesos_task):
        r = mesos_task.resources.add()
        r.name = self.name
        r.type = mesos_pb2.Value.SCALAR
        r.scalar.value = self.value

    def to_filter(self):
        return AirflowMesosResourceFilter(self.name, self.value)


class MesosHostnameResource(MesosResource):
    def __init__(self, value, exclude=False):
        super(MesosHostnameResource, self).__init__('hostname', value)
        self.exclude = exclude

    def add(self, mesos_task):
        pass

    def to_filter(self):
        l = self.value
        if not hasattr(self.value, '__iter__'):
            l = [self.value]
        return AirflowMesosHostnameFilter(l, self.exclude)


def default_resources():
    """
    [mesos]
    default_hostnames = [{"exclude": false, "value": ["hostname1", "hostname2"]}]
    default_resources = [{"type": "scalar", "name": "cpus", "value": 0.1}, {"type": "scalar", "name": "mem", "value": 64}]
    """
    def parse_config_value(configkey, default=None):
        if configuration.has_option('mesos', configkey):
            s = configuration.get('mesos', configkey)
            return json.loads(s)
        else:
            return default

    resources = []
    for kwargs in parse_config_value("DEFAULT_HOSTNAMES", []):
        resources.append(MesosHostnameResource(**kwargs))
    for kwargs in parse_config_value("DEFAULT_RESOURCES", []):
        type = kwargs.pop('type')
        if type == 'scalar':
            resources.append(MesosScalarResource(**kwargs))
        else:
            raise Exception('unknown type {0:s}'.format(kwargs['type']))

    return resources


def merge_resources(default_resources, task_resources):
    resources = list(default_resources)
    for task_resource in task_resources:
        for default_resource in default_resources:
            if task_resource.name == default_resource.name:
                resources.remove(default_resource)
        resources.append(task_resource)

    return resources
