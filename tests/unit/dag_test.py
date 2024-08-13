import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from unittest import TestCase
from classes import Dag


class DagTest(TestCase):
    def test_create_dag(self):
        d = Dag(
            "TestDag",
        )
        self.assertEqual("TestDag", d.name)
        self.assertEqual({}, d.table_lists)
        self.assertEqual({}, d.dataset_lists)
        self.assertEqual({}, d.project_lists)
        self.assertEqual([], d.tasks)
