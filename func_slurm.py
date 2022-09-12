# TODO document
# TODO higher API

import argparse
import pickle
import sys
import os
import inspect
import tempfile
import importlib
import warnings
import time
from pathlib import Path

from pslurm import Slurm
from pslurm import Status

MAX_NUM_OF_RETRIES = 5
MAX_MEMORY_TO_REQUEST = 100  # GB

use_slurm = True
buffered_printing = True


def disable_slurm():
    global use_slurm
    use_slurm = False


def disable_buffered_printing():
    # add '-u' to python command; makes prints show immediately
    global buffered_printing
    buffered_printing = False


class FuncSlurm:
    def __init__(self, func, *args, mem=1, slurm_flags='', **kwargs):
        # slurm_flags are additional flags to slurm, besides mem
        self.python_executable = sys.executable
        m = inspect.getmodule(func)
        depth = m.__name__.count('.')
        self.module_root = str(Path(m.__file__).parents[depth])
        self.module = m.__name__
        if self.module == '__main__':
            self.module = Path(inspect.getfile(func)).stem
        self.my_file = __file__
        self.func_name = func.__name__
        self.args = args
        self.kwargs = kwargs
        self.mem = mem
        self.slurm_flags = slurm_flags
        self.trial_num = 1
        self.result = None
        self.slurm = None
        if use_slurm:
            fd1, self.args_file = tempfile.mkstemp(prefix='pslurm_func_args_', suffix='.pickle.tmp', dir='.')
            fd2, self.results_file = tempfile.mkstemp(prefix='pslurm_func_results_', suffix='.pickle.tmp', dir='.')
            os.close(fd1)
            os.close(fd2)
            self.start()
        else:
            self.result = func(*self.args, **self.kwargs)

    def start(self):
        with open(self.args_file, 'wb') as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)
            if buffered_printing:
                python_flags = ''
            else:
                python_flags = '-u'
            self.slurm = Slurm(
                f'{self.python_executable} {python_flags} {self.my_file} --input_file {self.args_file} --output_file {self.results_file}',
                flags=f'--mem={self.mem}G {self.slurm_flags}')

    def restart(self):
        assert use_slurm
        self.trial_num += 1
        self.start()

    def wait_finished(self):
        self.slurm.wait_finished()

    def get_result(self, wait_finished=True):
        if not use_slurm:
            return self.result

        try:
            if wait_finished:
                self.wait_finished()
            try:
                status = self.slurm.get_status()
            except:
                status = None
        except:
            status = None

        if status == Status.COMPLETED:
            try:
                with open(self.results_file, 'rb') as f:
                    self.result = pickle.load(f)
                os.remove(self.results_file)
                # the args_file is also removed by wrapper(..)
                # however, because these are different machines it sometimes create a .nfs file
                # the double deletion will hopefully avoid this
                try:
                    os.remove(self.args_file)
                except FileNotFoundError:
                    # it was already deleted by wrapper(..)
                    pass
                return self.result
            except:
                raise RuntimeError("Failed to read results file: " + self.results_file)
        elif status == Status.OUT_OF_MEMORY:
            if self.mem >= MAX_MEMORY_TO_REQUEST:
                raise MemoryError("Job's memory isn't enough, raised up to " + str(self.mem) + "G")
            self.mem *= 2
            if self.mem > MAX_MEMORY_TO_REQUEST:
                self.mem = MAX_MEMORY_TO_REQUEST
            warnings.warn("Out of memory, increasing to " + str(self.mem) + "G")
            self.restart()
            return self.get_result(wait_finished)
        else:
            if self.trial_num < MAX_NUM_OF_RETRIES:
                warnings.warn(f"func_slurm: couldn't read status on trial #{self.trial_num}, restarting")
                time.sleep(2)  # wait 2 seconds, something might get synced, or otherwise improved, meanwhile
                self.restart()
                return self.get_result(wait_finished)
            else:
                print(self)
                raise RuntimeError("Job's status isn't COMPLETED. " + str(self.slurm))

    def __repr__(self):
        return "FuncSlurm({!r})".format({x: self.__dict__[x] for x in self.__dict__ if x not in ['args', 'kwargs']})


def wrapper(input_file, output_file):
    with open(input_file, 'rb') as f:
        job = pickle.load(f)
    os.remove(input_file)
    sys.path.insert(0, job.module_root)
    module = importlib.import_module(job.module)
    func = vars(module)[job.func_name]
    job.result = func(*job.args, **job.kwargs)
    with open(output_file, 'wb') as f:
        pickle.dump(job.result, f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Users should not run this directly")
    parser.add_argument("--input_file", help=argparse.SUPPRESS)
    parser.add_argument("--output_file", help=argparse.SUPPRESS)
    args = parser.parse_args()

    # print('SLAVE', os.environ['HOSTNAME'])  # TODO del
    if args.input_file and args.output_file:
        wrapper(args.input_file, args.output_file)
