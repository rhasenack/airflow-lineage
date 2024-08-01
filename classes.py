class PBN:

    def __init__(self, name) -> None:
        self.name = name
        self.dags = []
        self.resources = []
        self.tasks = []

    def add_dag(self, dag):
        dag.pbn = self
        self.dags.append(dag)

    def add_resource(self, resource):
        resource.pbn = self
        self.resources.append(resource)

    def add_task(self, task):
        task.pbn = self
        self.tasks.append(task)


class Resource:
    def __init__(self, name) -> None:
        self.name = name
        self.pbn = None
        self.script_content = None
        self.source_tables = []

    def get_source_tables(self):
        """This function shuld find all FROM and JOIN clauses in the script content of the resource and get the tablesc"""
        return


class Dag:
    def __init__(self, name) -> None:
        self.name = name
        self.pbn = None
        self.table_lists = []
        self.dataset_lists = []
        self.project_lists = []
        self.tasks = []

    def add_task(self, task):
        task.dag = self
        self.tasks.append()

    def add_table_list(self, table_list):
        self.table_lists.append(table_list)

    def add_project_list(self, project_list):
        self.project_lists.append(project_list)

    def add_dataset_list(self, dataset_list):
        self.dataset_lists.append(dataset_list)


class Task:
    def __init__(self, name, type, parameters) -> None:
        self.name = name
        self.dag = None
        self.type = type
        self.parameters = parameters
