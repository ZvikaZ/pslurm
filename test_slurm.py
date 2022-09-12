from unittest import TestCase
import os

import pslurm
from func_slurm import FuncSlurm


class TestSlurm(TestCase):
    def test_is_slurm_installed(self):
        self.assertTrue(pslurm.is_slurm_installed())

    def test_run(self):
        slurm = pslurm.Slurm('ls')
        self.assertEqual(pslurm.Status.SUBMITTED, slurm.status)

    def test_run_echo_hello_world(self):
        slurm = pslurm.Slurm("echo 'hello world'")
        self.assertEqual(pslurm.Status.SUBMITTED, slurm.status)
        slurm.wait_finished()
        self.assertEqual('hello world', slurm.get_output())

    def test_get_output(self):
        slurm = pslurm.Slurm('pwd')
        slurm.wait_finished()
        result = slurm.get_output()
        self.assertEqual(os.environ['PWD'], result)

    def test_get_status_ok(self):
        slurm = pslurm.Slurm('ls')
        slurm.wait_finished()
        self.assertEqual(pslurm.Status.COMPLETED, slurm.get_status())

    def get_status_failed(self):
        slurm = pslurm.Slurm('lszzzzzzzzzz')
        slurm.wait_finished()
        self.assertEqual(pslurm.Status.FAILED, slurm.get_status())

    def test_z_func_slurm(self):
        from func_slurm import use_slurm
        assert use_slurm
        job = FuncSlurm(check_func_slurm_helper, 3, 4, c=5)
        self.assertEqual({'name': 'checking', 'metadata': None, 'some_computation': 3 * 4 + 5}, job.get_result())

    def test_z_func_without_slurm(self):
        from func_slurm import disable_slurm
        disable_slurm()
        from func_slurm import use_slurm
        assert not use_slurm
        job = FuncSlurm(check_func_slurm_helper, 3, 4, c=5)
        self.assertEqual({'name': 'checking', 'metadata': None, 'some_computation': 3 * 4 + 5}, job.get_result())

    def test_z_func_slurm_restart(self):
        from func_slurm import use_slurm
        assert use_slurm
        job = FuncSlurm(check_func_slurm_helper, 3, 4, c=5)
        job.restart()
        self.assertEqual({'name': 'checking', 'metadata': None, 'some_computation': 3 * 4 + 5}, job.get_result())

    def test_z_func_slurm_wo_wait(self):
        from func_slurm import use_slurm
        assert use_slurm
        job = FuncSlurm(check_func_slurm_helper, 3, 4, c=5)
        self.assertRaises(RuntimeError, job.get_result, wait_finished=False)
        os.remove(job.results_file)
        os.remove(job.args_file)


def check_func_slurm_helper(a, b, c):
    print("check...")
    print(a, b, c)
    return {'name': 'checking', 'metadata': None, 'some_computation': a * b + c}
