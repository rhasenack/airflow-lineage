import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from unittest import TestCase
from unittest.mock import patch
from classes import PBN, Dag, Resource, Task


class TaskTest(TestCase):
    def test_define_dest_table_non_existing(self):
        t = Task(name="Test", type="TaskType")

        t.dest_table = "Dest Table"

        ## if there's no dataset, should return an error saying "dest_dataset not defined"
        self.assertRaises(AttributeError, t.define_dest_table)

    def test_define_dest_table_invalid(self):
        t = Task(name="Test", type="TaskType")

        t.dest_table = "Dest Table"
        t.dest_dataset = "ldw"
        t.write_disposition = "WRITE_APPEND"

        # Should not fail if there's dest_dataset and write_disposition
        try:
            t.define_dest_table()
        except AttributeError:
            self.fail("myFunc() raised ExceptionType unexpectedly!")

        t.write_disposition = "NON_VALID_VALUIE"

        # Should fail if write disposition is something that doesn't make sense
        self.assertRaises(AttributeError, t.define_dest_table)

    def test_define_dest_table_existing(self):

        t = Task(name="Test", type="TaskType")

        t.dest_table = "Dest Table"
        t.dest_dataset = "ldw"
        t.write_disposition = "WRITE_APPEND"

        t.define_dest_table()

        self.assertEqual(t.dest_table, "ldw.Dest Table")
        self.assertEqual(t.write_disposition, "INCREMENTAL")

    def test_define_source_tables_from_resource(self):

        t = Task(name="Test", type="TaskType")

        # if resource doesn't exist, raise attribute error
        self.assertRaises(AttributeError, t.define_dest_table)

        # if resource is a dml query, get the correct source table
        query = """
        INSERT INTO `_project-1_._dataset-1_._table-1_` SELECT 1 FROM  `_project-1_._dataset-1_._table-1_` 
        INNER JOIN SELECT 2 FROM  `_project-1_._dataset-1_._table-2_` ON 1=1
        """
        d = Dag("testDag")
        d.dataset_lists = {"dataset_list": {"_dataset-1_": "dataset1"}}
        d.table_lists = {"table_list": {"_table-1_": "table1", "_table-2_": "table2"}}
        t.dag = d

        r = Resource(name="r1", pbn="PBN", path="resource_path", script_content=query)
        t.resource = r
        t.dataset_list = "dataset_list"
        t.table_list = "table_list"
        t.define_dest_table()
        t.define_source_tables()

        self.assertEqual(t.source_tables, ["dataset1.table2"])

    def test_define_source_tables_from_resource_explicit(self):

        t = Task(name="Test", type="TaskType")

        # if resource doesn't exist, raise attribute error
        self.assertRaises(AttributeError, t.define_dest_table)

        # if resource is a dml query, get the correct source table
        query = """
        INSERT INTO `_project-1_._dataset-1_._table-1_` SELECT 1 FROM  `_project-1_._dataset-1_._table-1_` 
        INNER JOIN SELECT 2 FROM  `salesforce1.ldw.fact_case` ON 1=1
        """
        d = Dag("testDag")
        d.dataset_lists = {"dataset_list": {"_dataset-1_": "dataset1"}}
        d.table_lists = {"table_list": {"_table-1_": "table1", "_table-2_": "table2"}}
        t.dag = d

        r = Resource(name="r1", pbn="PBN", path="resource_path", script_content=query)
        t.resource = r
        t.dataset_list = "dataset_list"
        t.table_list = "table_list"
        t.define_dest_table()
        t.define_source_tables()

        self.assertEqual(t.source_tables, ["ldw.fact_case"])


## TESTS TO BUILD AND BUGS TO FIX:
## 1. Check if line is comment and ignore source tables in the sql code
## 2. Guarantee it works if there's no `` in table name.
