# TODO document
# TODO allow working without slurm

import argparse
import pickle
import sys
import os
import inspect
import tempfile
import importlib
import warnings
from pathlib import Path

from pslurm import Slurm
from pslurm import Status

MAX_NUM_OF_RETRIES = 5


class FuncSlurm:
    def __init__(self, func, *args, **kwargs):
        self.python_executable = sys.executable
        self.my_file = __file__
        self.func_file = inspect.getfile(func)
        self.func_name = func.__name__
        self.args = args
        self.kwargs = kwargs
        self.trial_num = 1
        self.result = None
        self.args_file = None
        self.results_file = None
        self.slurm = None
        self.start()

    def start(self):
        fd1, self.args_file = tempfile.mkstemp(prefix='pslurm_func_args_', suffix='.pickle.tmp', dir='.')
        fd2, self.results_file = tempfile.mkstemp(prefix='pslurm_func_results_', suffix='.pickle.tmp', dir='.')
        os.close(fd1)
        os.close(fd2)
        with open(self.args_file, 'wb') as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)
        self.slurm = Slurm(
            f'{self.python_executable} {self.my_file} --input_file {self.args_file} --output_file {self.results_file}')

    def restart(self):
        self.trial_num += 1
        self.start()

    def wait_finished(self):
        self.slurm.wait_finished()

    def get_result(self, wait_finished=True):
        if wait_finished:
            self.wait_finished()

        try:
            status = self.slurm.get_status()
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
        else:
            if self.trial_num < MAX_NUM_OF_RETRIES:
                warnings.warn(f"func_slurm: couldn't read status on trial #{self.trial_num}, restarting")
                self.restart()
                return self.get_result(wait_finished)
            else:
                print(self)
                raise RuntimeError("Job's status isn't COMPLETED. " + str(self.slurm))

    def __repr__(self):
        return "FuncSlurm({!r})".format(self.__dict__)


def wrapper(input_file, output_file):
    with open(input_file, 'rb') as f:
        job = pickle.load(f)
    os.remove(input_file)
    sys.path.insert(0, os.path.dirname(job.func_file))
    module = importlib.import_module(Path(job.func_file).stem)
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
