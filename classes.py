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

    def set_parameters(self, parameters: dict):
        for key, value in parameters.items():
            self.parameters[key] = value


# if it's a dml script, store: dest_table, file_name, task_id, write_disposition, table_list)
