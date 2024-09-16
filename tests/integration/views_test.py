import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from unittest import TestCase
from script import get_views
from classes import View


class ViewTest(TestCase):
    def test_get_views(self):
        views = get_views()

        view_names = []
        for view in views:
            view_names.append(view.table_name)

        # Has to have views
        self.assertGreater(len(views), 0)
        self.assertIn("fact_contact_workload", view_names)
        self.assertIn("fact_case_interaction", view_names)

    def test_fact_case_interaction(self):
        views = get_views()

        v = None
        for view in views:
            if view.table_name == "fact_case_interaction":
                v = view

        # Has to have views
        self.assertIn("ldw.fact_case_email", v.source_tables)
        self.assertIn("ldw.fact_case_social_post", v.source_tables)
        self.assertIn("ldw.fact_case", v.source_tables)
