import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from unittest import TestCase
from unittest.mock import patch
from classes import PBN, Dag, Resource, Task


class PbnTest(TestCase):
    def setUp(self):
        self.p = PBN(
            "TestPBN",
            """C:\\Users\\ricardo.hasenack\\Downloads""",
            """C:\\Users\\ricardo.hasenack\\Downloads""",
        )

        self.d = Dag(
            "TestDag",
        )

        self.r = Resource(
            name="resource",
            pbn=self.p,
            path="C:\\Users\\ricardo.hasenack\\Downloads",
            script_content="script_content",
        )

        self.r2 = Resource(
            name="resource2",
            pbn=self.p,
            path="C:\\Users\\ricardo.hasenack\\Downloads",
            script_content="script_content",
        )

        self.t = Task(name="task", type="taskType")
        self.t2 = Task(name="task2", type="taskType")

    def test_add_dags(self):
        self.p.add_dag(self.d)

        self.assertEqual(self.p.dags[0], self.d)
        self.assertEqual(len(self.p.dags), 1)

    def test_list_dags(self):

        self.p.add_dag(self.d)

        self.assertEqual(self.p.list_dags(), print("TestDag"))

    def test_list_dags_no_dag(self):

        with patch("builtins.print") as mocked_print:
            self.p.list_dags()
            mocked_print.assert_called_with("")

        self.p.add_dag(self.d)

        with patch("builtins.print") as mocked_print:
            self.p.list_dags()
            mocked_print.assert_called_with(self.d.name)

    def test_add_resource(self):
        self.p.add_resource(self.r)

        self.assertEqual(self.p.resources[0], self.r)

        # add another resource
        self.p.add_resource(self.r2)

        self.assertEqual(self.p.resources, [self.r, self.r2])

    def test_list_resources(self):

        self.p.add_resource(self.r)

        # add another resource
        self.p.add_resource(self.r2)

        with patch("builtins.print") as mocked_print:
            self.p.list_resources()
            mocked_print.assert_called_with(f"{self.r.name}, {self.r2.name}")

    def test_add_task(self):

        self.p.add_task(self.t)

        self.assertEqual(self.p.tasks[0], self.t)

        # add another task
        self.p.add_task(self.t2)

        self.assertEqual(self.p.tasks, [self.t, self.t2])

    def test_list_tasks(self):

        self.p.add_task(self.t)

        # add another task
        self.p.add_task(self.t2)

        with patch("builtins.print") as mocked_print:
            self.p.list_tasks()
            mocked_print.assert_called_with(f"{self.t.name}, {self.t2.name}")
