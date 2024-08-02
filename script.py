from classes import PBN, Resource, Task, Dag
from pathlib import Path
import os
import re
import ast


## Set Start Folder

start_path = Path(
    "C:\\Users\\ricardo.hasenack\\Documents\\data-core-customerexcellenceairflow\\dags\\teams\\operations"
)

## Iterate over subfolders and create a PBN object for each one of them
dags_folder = os.path.join(start_path, "dags")
resources_folder = os.path.join(start_path, "resources//bq_scripts")


PBNs = []
resources = []
tasks = []
dags = []

## For each one of these folders, create an instance of the PBN class
for folder in os.listdir(dags_folder):
    if folder[:9] == "data-core":
        pbn = PBN(
            name=folder,
            dag_pbn_path=os.path.join(dags_folder, folder),
            resources_pbn_path=os.path.join(resources_folder, folder),
        )
        PBNs.append(pbn)

## Go into each of these PBNs and go over each file. Each one of them will be a DAG class instance. Add DAG to the PBN.
for pbn in PBNs:
    # Iterate to get DAGs and Tasks
    for filename in os.listdir(pbn.dag_pbn_path):
        if filename.endswith(".py") and "-dq" not in filename:
            dag = Dag(name=filename)
            file_path = os.path.join(pbn.dag_pbn_path, filename)

            with open(file_path, "r") as file:
                tree = ast.parse(file.read(), filename=file_path)
            variables = {}

            # Let's do a MVP without Datasets and Projects because that is more complicated.
            regexp = re.compile(r"table_list(_[^,]+?\s|\s)=\s*({.+?})", re.S)

            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            var_name = target.id
                            if "table_list" in target.id:
                                variables[var_name] = ast.literal_eval(node.value)

                            # if it is calling a function and function name in 'run_dml_script' or 'run_raw_sql'
                            if (
                                isinstance(node, ast.Assign)
                                and isinstance(node.value, ast.Call)
                                and isinstance(node.value.func, ast.Name)
                                and (
                                    node.value.func.id == "run_dml_script"
                                    or node.value.func.id == "run_raw_sql"
                                )
                            ):
                                task_name = target.id
                                task_function = node.value.func.id
                                task = Task(name=task_name, type=task_function)
                                # if it's a dml script, store: dest_table, file_name, task_id, write_disposition, table_list)
                                parameters = {}
                                # if node.value.func.id == "run_dml_script":
                                for keyword in node.value.keywords:
                                    if isinstance(keyword.value, ast.Name):
                                        parameters[keyword.arg] = keyword.value.id
                                    if isinstance(keyword.value, ast.Constant):
                                        parameters[keyword.arg] = keyword.value.value
                                    if isinstance(keyword.value, ast.JoinedStr):
                                        if keyword.arg == "file_name":
                                            for value in keyword.value.values:
                                                if isinstance(value, ast.Constant):
                                                    parameters[keyword.arg] = (
                                                        value.value
                                                    )
                                task.set_parameters(parameters=parameters)
                                dag.add_task(task=task)

            dag.table_lists = variables

            dags.append(dag)
            pbn.add_dag(dag)

    # Iterate to get Resources
    for filename in os.listdir(pbn.resources_pbn_path):
        with open(os.path.join(pbn.resources_pbn_path, filename)) as f:
            file_content = f.read()
            resource = Resource(
                name=filename,
                path=os.path.join(pbn.resources_pbn_path, filename),
                pbn=pbn,
                script_content=file_content,
            )
        x = 1
        pbn.add_resource(resource)
        resources.append(resource)

print(PBNs)
