"""Small importable workers used to test process scheduling without model data."""
import os
import time


def scheduler_probe(task):
    time.sleep(0.1)
    if task.get('crash'):
        os._exit(7)
    if task.get('fail'):
        raise RuntimeError('intentional worker failure')
    return {'task_id': task['task_id'], 'member': task['member'], 'status': 'completed',
            'pid': os.getpid(), 'execution': {}, 'value': task.get('value', 0) * 2,
            'native_threads': os.environ.get('OMP_NUM_THREADS')}
