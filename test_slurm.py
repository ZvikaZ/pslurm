from unittest import TestCase
import os

import pslurm

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

    def get_status_faile(self):
        slurm = pslurm.Slurm('lszzzzzzzzzz')
        slurm.wait_finished()
        self.assertEqual(pslurm.Status.FAILED, slurm.get_status())

