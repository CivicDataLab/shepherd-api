import datetime
import glob
import logging


def set_log_file(pipeline_id, pipeline_name):
    now = datetime.datetime.now()
    log_file_name = f'logs/{pipeline_id}_{pipeline_name}_{now.strftime("%d_%m_%Y-%H_%M_%S")}.log'
    logger = logging.getLogger("IDP-Datapipeline")
    hdlr = logging.StreamHandler()
    fhdlr = logging.FileHandler(log_file_name)
    logger.addHandler(hdlr)
    logger.addHandler(fhdlr)
    logger.setLevel(logging.INFO)
    return logger


def get_logger_for_existing_file(pipeline_id):
    log_file = glob.glob(f'logs/{pipeline_id}*.log')[0]
    logger = logging.getLogger("IDP-Datapipeline")
    hdlr = logging.StreamHandler()
    fhdlr = logging.FileHandler(log_file)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        logger.addHandler(hdlr)
        logger.addHandler(fhdlr)
    return logger
