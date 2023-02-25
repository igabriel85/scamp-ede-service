from ede_engine import EDEScampEngine
import time
from rq import get_current_job
import os
import numpy as np
np.random.seed(42)

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
etc_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
model_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')



def ede_detect_handler(mconf):
    '''
    :param mconf: merged config file containg all configuration files
        :p ede_cfg: ede configuration file
        :p source_cfg: source configuration file
        :p loop: if True, the detection will be run in a loop
        :p period: the period of the loop
    '''

    ede_cfg = mconf['ede_cfg']
    source_cfg = mconf['source_cfg']
    loop = mconf['loop']
    period = mconf['period']
    job = get_current_job()
    job.meta['progress'] = "Started detection"
    ede = EDEScampEngine(ede_cfg, source_cfg, data_dir, etc_dir, model_dir, job=job)
    if loop:
        while True:
            ede.detect()
            time.sleep(period)
    else:
        ede.detect()
        job.meta['progress'] = "Finished detection"


def ede_anomaly_trainer_handler(mconf):
    '''
    :param mconf: merged config file containg all configuration files
        :p ede_cfg: ede configuration file
        :p source_cfg: source configuration file
    '''

    ede_cfg = mconf['ede_cfg']
    source_cfg = mconf['source_cfg']
    job = get_current_job()
    ede = EDEScampEngine(ede_cfg, source_cfg, data_dir, etc_dir, model_dir, job=job)
    job.meta['progress'] = "Started anomaly method training"
    ede.cycle_anomaly_trainer()
    job.meta['progress'] = "Finished anomaly method training"




