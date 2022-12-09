# ede_worker.py
from redis import Redis
from rq import Worker, Queue, Connection
from utils import load_yaml, save_pid, clean_up_pid

r_connection = Redis(host='redis', port=6379)
if __name__ == '__main__':
    config = load_yaml('worker.yaml')
    with Connection(connection=r_connection):
        worker = Worker(map(Queue, config['listen']))
        pid_f = 'worker_{}.pid'.format(worker.name)
        print('Saving pid {} to file {}'.format(worker.pid, pid_f))
        save_pid(worker.pid, pid_f)
        worker.work()
        print('Cleaning up ...')
        clean_up_pid(pid_f)