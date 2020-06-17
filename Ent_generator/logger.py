import logging

file_path = 'Ent_generator/log_file1.log'

logger = logging.getLogger(__name__) #Allow us to work with different log classes
logger.propagate = False
logger.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
fh = logging.FileHandler(file_path)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.propagate = False

logger.setLevel(logging.ERROR)
logger.setLevel(logging.INFO)