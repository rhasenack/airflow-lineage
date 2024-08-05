import sqlparse
import re


class PBN:
    def __str__(self):
        return f"PBN named {self.name} with {len(self.dags)} dags"

    def __repr__(self):
        return f"PBN named {self.name} with {len(self.dags)} dags"

    def __init__(self, name, dag_pbn_path, resources_pbn_path) -> None:
        self.name = name
        self.dags = []
        self.resources = []
        self.tasks = []
        self.dag_pbn_path = dag_pbn_path
        self.resources_pbn_path = resources_pbn_path

    def add_dag(self, dag):
        dag.pbn = self
        self.dags.append(dag)

    def list_dags(self):
        print(", ".join(dag.name for dag in self.dags))

    def add_resource(self, resource):
        resource.pbn = self
        self.resources.append(resource)

    def list_resources(self):
        print(", ".join(resource.name for resource in self.resources))

    def add_task(self, task):
        task.pbn = self
        self.tasks.append(task)

    def list_tasks(self):
        print(", ".join(task.name for task in self.tasks))


class Resource:
    def __init__(self, name, pbn, path, script_content) -> None:
        self.name = name
        self.pbn = pbn
        self.path = path
        self.script_content = script_content
        self.source_tables = []

    def get_source_tables(self):
        """This function shuld find all FROM and JOIN clauses in the script content of the resource and get the tablesc"""
        return


class Dag:
    def __init__(self, name) -> None:
        self.name = name
        self.pbn = None
        self.resource = None
        self.table_lists = {}
        self.dataset_lists = {}
        self.project_lists = {}
        self.tasks = []

    def add_task(self, task):
        task.dag = self
        self.tasks.append(task)

    def add_table_list(self, table_list):
        self.table_lists.append(table_list)

    def add_project_list(self, project_list):
        self.project_lists.append(project_list)

    def add_dataset_list(self, dataset_list):
        self.dataset_lists.append(dataset_list)


class Task:
    def __init__(self, name: str, type: str) -> None:
        self.name = name
        self.dag = None
        self.type = type
        self.parameters = {}
        self.source_tables = []

    def set_parameters(self, parameters: dict):
        for dict_key, value in parameters.items():
            setattr(self, dict_key, value)

    def extract_table_name(self, token_list):
        for token in token_list:
            if isinstance(token, sqlparse.sql.Identifier):
                return token.get_real_name()
            elif isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    return identifier.get_real_name()
        return None

    def define_dest_table(self):
        if self.resource is None:
            print("Resource not assinged to task")
            return

        if hasattr(self, "dest_table"):
            print("destination table already set")
            return

        else:
            sql = self.resource.script_content
            regexp = re.compile(
                r".*?(INSERT|UPDATE|DELETE|MERGE|CREATE .+?TABLE).+?\s+.?(`.+?`)",
                flags=re.S | re.I,
            )

            match = re.match(regexp, sql)

            dml_type = match.__getitem__(1)
            self.type = dml_type

            det_path_string = match.__getitem__(2)

            tbl_regexp = re.compile(
                r"\`_project-\d{1,2}_\._dataset-\d{1,2}_\.(_table-\d{1,2}_|.+?)\`"
            )

            match = re.match(tbl_regexp, det_path_string)
            dest_table_string = match.__getitem__(1)
            if "_table-" in dest_table_string:
                self.dest_table = self.dag.table_lists[self.table_list][
                    dest_table_string
                ]
            else:
                self.dest_table = dest_table_string

    def define_source_tables(self):
        if self.resource is None:
            print("Resource not assinged to task")
            return

        else:
            sql = self.resource.script_content
            regexp = re.compile(
                r"\`_project-\d{1,2}_\._dataset-\d{1,2}_\.(_table-\d{1,2}_|.+?)\`"
            )

            matches = re.findall(regexp, sql)

            for match in matches:
                if "_table-" in match:
                    self.source_tables.append(
                        self.dag.table_lists[self.table_list][match]
                    )
                else:
                    self.dest_table = match


# if it's a dml script, store: dest_table, file_name, task_id, write_disposition, table_list)
