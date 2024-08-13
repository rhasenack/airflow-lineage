import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from unittest import TestCase
from classes import PBN


class PbnTest(TestCase):
    def test_create_pbn(self):
        p = PBN(
            "TestPBN",
            """C:\\Users\\ricardo.hasenack\\Downloads""",
            """C:\\Users\\ricardo.hasenack\\Downloads""",
        )
        self.assertEqual("TestPBN", p.name)
        self.assertEqual("""C:\\Users\\ricardo.hasenack\\Downloads""", p.dag_pbn_path)
        self.assertEqual(
            """C:\\Users\\ricardo.hasenack\\Downloads""", p.resources_pbn_path
        )

    def test_repr(self):
        p = PBN(
            "TestPBN",
            """C:\\Users\\ricardo.hasenack\\Downloads""",
            """C:\\Users\\ricardo.hasenack\\Downloads""",
        )

        expected = "PBN named TestPBN with 0 dags"

        self.assertEqual(expected, p.__repr__())
