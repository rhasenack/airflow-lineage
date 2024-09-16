import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from unittest import TestCase
from unittest.mock import patch
from classes import PBN, Dag, Resource, Task


class DagTest(TestCase):
    def test_add_task(self):
        d = Dag("Test")
        t = Task(name="Test", type="TaskType")
        t2 = Task(name="Test", type="TaskType")

        d.add_task(t)

        self.assertEqual(d.tasks[0], t)

        d.add_task(t2)

        self.assertEqual(d.tasks, [t, t2])
