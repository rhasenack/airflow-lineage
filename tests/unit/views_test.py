import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from unittest import TestCase
from script import get_views
from classes import View


class ViewTest(TestCase):
    def test_view(self):
        table_name = "project.dataset.test_view"
        sql_script = "SELECT * FROM project.dataset.other_table_name T LEFT JOIN other_project.other_dataset.last_table_name"

        v = View(table_name=table_name, sql_script=sql_script)
        self.assertEqual(v.table_name, table_name)
        self.assertEqual(v.sql_script, sql_script)
        self.assertEqual(v.source_tables, [])

    def test_define_source_tables(self):
        table_name = "project.dataset.test_view"
        sql_script = "SELECT * FROM `project.dataset.other_table_name` T LEFT JOIN `other_project.other_dataset.last_table_name`"

        v = View(table_name=table_name, sql_script=sql_script)
        v.define_source_tables()

        self.assertEqual(v.source_tables[0], "project.dataset.other_table_name")
        self.assertEqual(
            v.source_tables[1], "other_project.other_dataset.last_table_name"
        )
