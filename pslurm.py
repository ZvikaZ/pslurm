from enum import Enum, auto
import re
import time
import subprocess


class Status(Enum):
    INIT = auto()
    SUBMITTED = auto()
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    CONFIGURING = auto()
    OUT_OF_MEMORY = auto()


submit_pattern = re.compile(r'Submitted batch job (\d+)')
output_pattern = re.compile(r'.*StdOut=(.+?) .*')
scontrol_status_pattern = re.compile(r'.*JobState=(.+?) .*')


def run_command(command):
    p = subprocess.run([command], shell=True, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr + "\n" + command)
    return p.stdout


def read_file_to_string(filename):
    with open(filename, 'r') as f:
        return f.read().rstrip()


def is_slurm_installed():
    return run_command('which sbatch') is not None


class Slurm:
    def __init__(self, command, flags='', name=None):
        self.delay_between_status_checks = 0.3  # second
        self.status = Status.INIT
        self.command = command
        self.job_id = None
        self.output_file_name = None
        self.flags = flags
        if name is None:
            self.name = 'pslurm'
        else:
            self.name = name
        self.run_job()

    def __repr__(self):
        return f'Slurm. jobID: {self.job_id}, last status: {self.status}. command: {self.command}'

    def has_failed(self):
        return self.status in [Status.FAILED, Status.OUT_OF_MEMORY, Status.CANCELLED]

    def run_job(self):
        result = run_command(f'sbatch --job-name={self.name} {self.flags} --wrap="{self.command}"')
        if result is None:
            self.status = Status.FAILED
        else:
            m = submit_pattern.search(result)
            if m:
                self.status = Status.SUBMITTED
                self.job_id = int(m.group(1))
                self.update_output_file_name()
            else:
                self.status = Status.FAILED

    def wait_finished(self):
        self.update_status()
        while self.hasnt_finished():
            time.sleep(self.delay_between_status_checks)
            self.update_status()

    def hasnt_finished(self):
        return self.status != Status.COMPLETED and not self.has_failed()

    def update_output_file_name(self):
        result = run_command(f'scontrol show job {self.job_id} -o')
        if result is None:
            self.status = Status.FAILED
        else:
            m = output_pattern.search(result)
            if m:
                self.output_file_name = m.group(1)
            else:
                self.status = Status.FAILED

    def get_status(self):
        self.update_status()
        return self.status

    def get_output_file_name(self):
        return self.output_file_name

    def get_output(self):
        return read_file_to_string(self.get_output_file_name())

    def update_status(self):
        if self.hasnt_finished():
            # scontrol has disadvantage that it's only for running, or recently finished
            # sacct has disadvantage that it doesn't work on some of our machines
            # currently using scontrol
            # if sacct is needed, see usage reference at jslurm
            state = None
            result = run_command(f'scontrol show job {self.job_id}')
            m = scontrol_status_pattern.search(result)
            if m:
                state = m.group(1)
                if state == "COMPLETED":
                    self.status = Status.COMPLETED
                elif state == "FAILED":
                    self.status = Status.FAILED
                elif state == "PENDING":
                    self.status = Status.PENDING
                elif state == "RUNNING":
                    self.status = Status.RUNNING
                elif state == "COMPLETING":
                    # for my needs they are identical
                    self.status = Status.RUNNING
                elif state == "CANCELLED":
                    self.status = Status.CANCELLED
                elif state == "CONFIGURING":
                    self.status = Status.CONFIGURING
                elif state == "OUT_OF_MEMORY":
                    self.status = Status.OUT_OF_MEMORY
                else:
                    print(f"pslurm: Unrecognized status: {state}, job: {self.job_id}")
