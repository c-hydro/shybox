"""Bounded process scheduling; no live SHYBOX or NetCDF objects cross processes."""
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from contextlib import contextmanager
import multiprocessing
import os
import traceback


@contextmanager
def native_thread_limits(count):
    names = ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS')
    previous = {name: os.environ.get(name) for name in names}
    try:
        for name in names:
            os.environ[name] = str(count)
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def iter_task_results(tasks, worker, *, max_workers=2, native_threads=1):
    """Yield (task, result) as jobs finish, retaining at most 2*workers futures.

    The worker must be an importable top-level function. Spawn avoids inheriting
    open NetCDF handles, logging locks or initialized numerical-library state.
    A worker exception becomes a failed task report; remaining jobs still run.
    """
    for name, value in (('max_workers', max_workers), ('native_threads', native_threads)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f'{name} must be a positive integer.')
    tasks = iter(tasks)
    with native_thread_limits(native_threads):
        with ProcessPoolExecutor(max_workers=max_workers,
                                 mp_context=multiprocessing.get_context('spawn')) as pool:
            pending = {}
            unsubmitted = []
            submission_error = None

            def fill_queue():
                nonlocal submission_error
                while len(pending) < max_workers * 2:
                    try:
                        task = next(tasks)
                    except StopIteration:
                        break
                    try:
                        pending[pool.submit(worker, task)] = task
                    except BrokenProcessPool as exc:
                        submission_error = exc
                        unsubmitted.append(task)
                        break

            fill_queue()
            while pending:
                finished, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in finished:
                    task = pending.pop(future)
                    try:
                        result = future.result()
                    except Exception as exc:
                        if isinstance(exc, BrokenProcessPool):
                            submission_error = exc
                        result = {
                            'task_id': task['task_id'], 'member': task['member'],
                            'status': 'failed', 'execution': {},
                            'error': f'{type(exc).__name__}: {exc}',
                            'traceback': traceback.format_exc(),
                        }
                    yield task, result
                if submission_error is None:
                    fill_queue()
            if submission_error is not None:
                # A crashed pool cannot accept new jobs. Report every remaining
                # job as failed rather than losing it or retrying writes silently.
                for task in unsubmitted:
                    yield task, {'task_id': task['task_id'], 'member': task['member'],
                                 'status': 'failed', 'execution': {},
                                 'error': f'Process pool unavailable: {submission_error}'}
                for task in tasks:
                    yield task, {'task_id': task['task_id'], 'member': task['member'],
                                 'status': 'failed', 'execution': {},
                                 'error': f'Process pool unavailable: {submission_error}'}
