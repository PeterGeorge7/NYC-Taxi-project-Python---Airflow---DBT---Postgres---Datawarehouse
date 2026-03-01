import logging


def get_logger(name):
    # Airflow automatically captures everything from the 'airflow.task' logger
    logger = logging.getLogger(f"airflow.task.{name}")
    return logger
