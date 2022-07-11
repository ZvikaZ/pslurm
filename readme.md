# pslurm
Simple, no installation, thin Slurm wrapper for Python.

## Why?
Unlike other slurm wrappers, *pslurm* doesn't need installation, and has a very quick learning curve.

## What can it do?
*pslurm* can *send* job, *monitor* their progress, and *retrieve* their output

### Note
This package is still in early development stages.

## Requirements
- Unix
- Python 3.7 and above.
(if you really need older version support, I can easily make the required modifications, just contact me)
- Slurm installed on your machine, with the following commands in PATH:
    - `sbatch`
    - `scontrol`

## Installation

Just copy `pslurm.py`, or, if you prefer *pip*:

    pip install git+https://github.com/ZvikaZ/pslurm

## Usage

    import pslurm

    # sanity check, verify that Slurm is really installed in your machine
    assert pslurm.is_slurm_installed()
    
    # send a job
    slurm = pslurm.Slurm("echo 'hello world'")
    
    # you don't *have* to make this check
    if pslurm.Status.SUBMITTED != slurm.status:
        print("something is wrong")
    
    # this echo takes no time, so this `wait_finished` might not seem important here;
    # however, for other jobs, it's important to wait for them to finish
    slurm.wait_finished()
    
    # you probably always want to make this check
    if pslurm.Status.COMPLETED != slurm.get_status():
        # maybe try to send few times, and only then issue an error
        print("something is wrong")
    
    # get job's output
    print(slurm.get_output())   # "hello world"
    
    # demonstration of a failed job, running an unexistant (at least, in my system) command:
    slurm = pslurm.Slurm('lszzzzzzzzzz')
    slurm.wait_finished()
    assert pslurm.has_failed()
    
### Notes

A job is submitted by creating a new `Slurm` class, initialized with the command to execute, together with its parameters, as a single string. The returned object is used to monitor the job and retrieve the job's output.

The job isn't monitored in the background. In order to update its `status`, you should call `get_status()` (which also returns the updated `status` field).

