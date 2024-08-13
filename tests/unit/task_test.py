import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from unittest import TestCase
from classes import Task


class TaskTest(TestCase):
    def test_create_Task(self):
        d = Task("TestTask", "run_raw_SQL_script")
        self.assertEqual("TestTask", d.name)
        self.assertEqual("run_raw_SQL_script", d.type)
