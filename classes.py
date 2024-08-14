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


class Task:
    def __init__(self, name: str, type: str) -> None:
        self.name = name
        self.dag = None
        self.type = type
        self.bq_parameters = None
        self.parameters = {}
        self.source_tables = []

    def set_parameters(self, parameters: dict):
        for dict_key, value in parameters.items():
            setattr(self, dict_key, value)

    def define_dest_table(self):
        """Define the destination table at the task. Depending on the task, it can be a direct parameter of the function or it might have to be fetched from the SQL script that is called
        A given task will have only one dest_table"""

        # If there's a function parameter called dest_table, this is directly the task's dest_table
        if hasattr(self, "dest_table"):
            # print("destination table already set")

            if not hasattr(self, "dest_dataset"):
                raise AttributeError("No dest_dataset defined")

            dest_dataset = (
                self.dag.bq_parameters[self.dest_dataset]
                if "BQ_" in self.dest_dataset
                else self.dest_dataset
            )
            dest_table = self.dest_table
            self.dest_table = dest_dataset + "." + self.dest_table

            if self.type != "run_copy_table_dml":
                if not hasattr(self, "write_disposition"):
                    raise AttributeError("No write_disposition defined")
                if self.write_disposition not in ["WRITE_APPEND", "WRITE_TRUNCATE"]:
                    raise AttributeError("write_disposition incorrectly defined")

            # Set DML type
            if self.type != "run_copy_table_dml":
                self.write_disposition = (
                    "INCREMENTAL"
                    if self.write_disposition == "WRITE_APPEND"
                    else "FULL REFRESH"
                )
            else:
                self.write_disposition = "COPY"
            return

        # If there's no resoucre assigned to the task, we're unable to get the dest_table from the sql script. This, in theory, shouldn't happen.
        if not hasattr(self, "resource") or self.resource is None:
            raise AttributeError("No Resource Defined")

        # If not in previous scenarios, he table is fetched by looking for dml statements and the tables with REGEX.
        else:
            sql = self.resource.script_content
            regexp = re.compile(
                r".*?(INSERT|UPDATE|DELETE|MERGE|CREATE .+?TABLE).+?\s+.?(`.+?`)",
                flags=re.S | re.I,
            )

            match = re.match(regexp, sql)

            dml_statement = match.__getitem__(1)
            self.dml_statement = dml_statement

            det_path_string = match.__getitem__(2)

            tbl_regexp = re.compile(
                r"\`(_project-\d{1,2}_|.+?)\.(_dataset-\d{1,2}_|.+?)\.(_table-\d{1,2}_|.+?)\`"
            )

            match = re.match(tbl_regexp, det_path_string)
            dest_project_string = match.__getitem__(1)
            dest_dataset_string = match.__getitem__(2)
            dest_table_string = match.__getitem__(3)

            if "_dataset-" in dest_dataset_string:
                dest_dataset = self.dag.dataset_lists[self.dataset_list][
                    dest_dataset_string
                ]
            else:
                dest_dataset = dest_dataset_string

            if "_table-" in dest_table_string:
                dest_table = self.dag.table_lists[self.table_list][dest_table_string]
            else:
                dest_table = dest_table_string

            self.dest_table = dest_dataset + "." + dest_table

        # Set Target Type
        if any(x in self.dml_statement.upper() for x in ["CREATE"]):
            self.write_disposition = "FULL REFRESH"
        elif any(
            x in self.dml_statement.upper() for x in ["INSERT", "UPDATE", "MERGE"]
        ):
            self.write_disposition = "INCREMENTAL"
        elif any(x in self.dml_statement.upper() for x in ["DELETE"]):
            self.write_disposition = "DELETE"

    def define_source_tables(self):
        """Define the source tables of the task. Depending on the task, it can be a direct parameter of the function or it might have to be fetched from the SQL script.
        A given task will have only one destination, but might have several sources"""

        if not hasattr(self, "dest_table"):
            raise AttributeError("No destination table set")

        # If there's a function parameter called dest_table, this is directly the task's dest_table
        if hasattr(self, "source_table"):
            self.source_tables.append(self.source_dataset + "." + self.source_table)
            return

        # If there's no resoucre assigned to the task, we're unable to get the source_tables from the sql script. This, in theory, shouldn't happen.
        if not hasattr(self, "resource") or self.resource is None:
            raise AttributeError("No Resource Defined")

        # Else, using regex, we get the tables that are referenced in the SQL script.
        sql = self.resource.script_content
        regexp = re.compile(
            r"(?:\`{0,1}|\s)(_project-\d{1,2}_|\w+?)\.(_dataset-\d{1,2}_|\w+?)\.(_table-\d{1,2}_|\w+)(?:\`{0,1}|\s)"
        )

        # Remove commented lines that shouldn't be consideres
        rexep_commented = re.compile(r"(\-\-.+?\n)")
        sql = re.sub(pattern=rexep_commented, repl="", string=sql)

        matches = re.findall(regexp, sql)

        for match in matches:
            source_project_string = match.__getitem__(0)
            source_dataset_string = match.__getitem__(1)
            source_table_string = match.__getitem__(2)

            if "_dataset-" in source_dataset_string:
                if source_dataset_string == "_dataset-dw_":

                    source_dataset = "gold_raw"
                else:
                    source_dataset = self.dag.dataset_lists[self.dataset_list][
                        source_dataset_string
                    ]
            else:
                source_dataset = source_dataset_string

            if "_table-" in source_table_string:

                if source_table_string == "_table_name_dest_":
                    source_table = "dimcontactgroup_stg_all"
                else:
                    source_table = self.dag.table_lists[self.table_list][
                        source_table_string
                    ]
            else:
                source_table = source_table_string

            ## If source table is the same as the dest table, ignore
            if source_dataset + "." + source_table != self.dest_table:
                self.source_tables.append(source_dataset + "." + source_table)
