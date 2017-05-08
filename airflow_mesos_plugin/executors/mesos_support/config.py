import logging

from airflow import configuration, AirflowException
from airflow.settings import Session

DEFAULT_FRAMEWORK_NAME = 'airflow'
FRAMEWORK_CONNID_PREFIX = 'mesos_framework_'


def config_framework_name():
    if not configuration.has_option('mesos', 'FRAMEWORK_NAME'):
        return DEFAULT_FRAMEWORK_NAME

    return configuration.get('mesos', 'FRAMEWORK_NAME')


def config_framework_connid():
    return FRAMEWORK_CONNID_PREFIX + config_framework_name()


def config_master_url():
    if not configuration.has_option('mesos', 'MASTER'):
        logging.error("Expecting mesos master URL for mesos executor")
        raise AirflowException(
            "mesos.master not provided for mesos executor")

    return configuration.get('mesos', 'MASTER')


def config_framework_checkpoint():
    if not configuration.has_option('mesos', 'CHECKPOINT'):
        return False

    return configuration.getboolean('mesos', 'CHECKPOINT')


def config_framework_failover_timeout():
    if not configuration.has_option('mesos', 'FAILOVER_TIMEOUT'):
        return None

    return configuration.get('mesos', 'FAILOVER_TIMEOUT')


def framework_id_for_failover(frameworkId=None):
    # Import here to work around a circular import error
    from airflow.models import Connection

    # Query the database to get the ID of the Mesos Framework, if available.
    conn_id = config_framework_connid()
    session = Session()
    connection = session \
        .query(Connection) \
        .filter_by(conn_id=conn_id) \
        .first()

    if frameworkId:
        # Setting
        if connection is None:
            connection = Connection(conn_id=conn_id,
                                    conn_type=FRAMEWORK_CONNID_PREFIX + "_id",
                                    extra=frameworkId.value)
        else:
            connection.extra = frameworkId.value

        session.add(connection)
        session.commit()
        Session.remove()

    else:
        # Retrieving
        if connection is None:
            return None
        else:
            return connection.extra

def config_framework_authenticate():
    if not configuration.has_option('mesos', 'AUTHENTICATE') or \
            not configuration.getboolean('mesos', 'AUTHENTICATE'):
        return None

    if not configuration.has_option('mesos', 'DEFAULT_PRINCIPAL'):
        logging.error(
            "Expecting authentication principal in the environment")
        raise AirflowException(
            "mesos.default_principal not provided in authenticated mode")
    if not configuration.has_option('mesos', 'DEFAULT_SECRET'):
        logging.error(
            "Expecting authentication secret in the environment")
        raise AirflowException(
            "mesos.default_secret not provided in authenticated mode")

    return (configuration.get('mesos', 'DEFAULT_PRINCIPAL'),
            configuration.get('mesos', 'DEFAULT_SECRET'))
