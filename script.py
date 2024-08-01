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

PBNs = []
resources = []
tasks = []
dags = []

## For each one of these folders, create an instance of the PBN class
for folder in os.listdir(dags_folder):
    if folder[:9] == "data-core":
        pbn = PBN(name=folder, dag_pbn_path=os.path.join(dags_folder, folder))
        PBNs.append(pbn)

## Go into each of these PBNs and go over each file. Each one of them will be a DAG class instance. Add DAG to the PBN.
for pbn in PBNs:
    for filename in os.listdir(pbn.dag_pbn_path):
        if filename.endswith(".py") and "-dq" not in filename:
            dag = Dag(name=filename)
            file = open(os.path.join(pbn.dag_pbn_path, filename))

            content = file.read()
            print(filename)
            table_lists = re.findall(r"table_list\s=\s*({.+?})", content, re.S)
            dataset_lists = re.findall(r"dataset_list\s=\s*({.+?})", content, re.S)
            project_lists = re.findall(r"project_list\s=\s*({.+?})", content, re.S)

            for table_list in table_lists:
                dag.table_lists.append(ast.literal_eval(table_list))

            # for dataset_list in dataset_lists:
            #     dag.dataset_lists.append(ast.literal_eval(dataset_list))

            # for project_list in project_lists:
            #     dag.project_lists.append(ast.literal_eval(project_list))

            dags.append(dag)
            pbn.add_dag(dag)

            file.close()

print(PBNs)


## Also, for each of these PBNs, create the resource class. Add resource to the PBN

## For each one of this DAGs, look for the relevant Tasks (run_dml_script or run_raw_sql) and create a Task instance. Add task to the DAG
