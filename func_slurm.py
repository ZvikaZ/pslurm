# TODO document
# TODO try few times if fails
# TODO allow working without slurm

import argparse
import pickle
import sys
import os
import inspect
import tempfile
import importlib
from pathlib import Path

from pslurm import Slurm
from pslurm import Status


class FuncSlurm:
    def __init__(self, func, *args, **kwargs):
        self.python_executable = sys.executable
        self.my_file = __file__
        self.func_file = inspect.getfile(func)
        self.func = func.__name__
        self.args = args
        self.kwargs = kwargs
        _, self.args_file = tempfile.mkstemp(prefix='pslurm_func_args_', suffix='.pickle.tmp', dir='.')
        _, self.results_file = tempfile.mkstemp(prefix='pslurm_func_results_', suffix='.pickle.tmp', dir='.')
        self.result = None
        with open(self.args_file, 'wb') as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)
        self.slurm = Slurm(f'{self.python_executable} {self.my_file} --input_file {self.args_file} --output_file {self.results_file}')

    def wait_finished(self):
        self.slurm.wait_finished()

    def get_result(self, wait_finished=True):
        if wait_finished:
            self.wait_finished()

        if self.slurm.get_status() == Status.COMPLETED :
            try:
                with open(self.results_file, 'rb') as f:
                    self.result = pickle.load(f)
                    os.remove(self.args_file)
                    os.remove(self.results_file)

                    return self.result
            except:
                raise RuntimeError("Failed to read results file: " + self.results_file)
        else:
            raise RuntimeError("Job's status isn't COMPLETED. " + str(self.slurm))

    def __repr__(self):
        return "FuncSlurm({!r})".format(self.__dict__)


def wrapper(input_file, output_file):
    with open(input_file, 'rb') as f:
        job = pickle.load(f)
    sys.path.insert(0, os.path.dirname(job.func_file))
    module = importlib.import_module(Path(job.func_file).stem)
    func = vars(module)[job.func]
    job.result = func(*job.args, **job.kwargs)
    with open(output_file, 'wb') as f:
        pickle.dump(job.result, f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Users should not run this directly")
    parser.add_argument("--input_file", help=argparse.SUPPRESS)
    parser.add_argument("--output_file", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.input_file and args.output_file:
        wrapper(args.input_file, args.output_file)
