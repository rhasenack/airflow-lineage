from classes import PBN, Resource, Task, Dag
from pathlib import Path
import os
import re
import ast
import json
import networkx as nxpi
from pyvis.network import Network


## Set Start Folder
start_path = Path(
    "C:\\Users\\ricardo.hasenack\\Documents\\data-core-customerexcellenceairflow\\dags\\teams\\operations"
)


# Initialize varables in which we'll store the classes
PBNs = []
resources = []
tasks = []
dags = []

# SF Parameeters used on the dags
AIRFLOW_VAR_BI_OPERATIONS_SALESFORCE_PARAMETERS = json.loads(
    '{"PROJECT_NAME": "BI-Salesforce","BI_SALESFORCE_DEBUG_MODE": 0,"ERROR_MAIL_OPS": "dl-pt-ftech-bi-operations@farfetch.com","ERROR_MAIL_DQ_P5": "joaocerca.santos@farfetch.com,miguel.duarte@farfetch.com","GCP_BQ_CON_ID": "prd-data-ldw-salesforce-1","BQ_PROJECT_WB_ID": "bigquery-analytics-workbench","BQ_PROJECT_WFOPS_ID": "prd-data-teams-wfops-1","BQ_PROJECT_WB_VIEWS": "bigquery-analytics-workbench","BQ_DATASET_WFOPS_LZ": "landing_zone","BQ_DATASET_STAGING": "gold_raw","BQ_DATASET_EXTERNAL": "external","BQ_DATASET_SALESFORCE": "salesforce","BQ_DATASET_WFM": "wfm","BQ_DATASET_DW": "gold_read","BQ_DATASET_WB": "gold_read","BQ_DATASET_ODS": "ods","BQ_DATASET_DQ": "data_quality","BQ_DATASET_WFOPS": "forecast","BQ_DATASET_LDW": "ldw","FORECAST_BACKFILL_START_DATE": "2021-07-22","FORECAST_BACKFILL_END_DATE": "2021-09-01","BI_SALESFORCE_GCP_PROJECT": "prd-data-ldw-salesforce-1","BQ_SALESFORCE_SERVICE_ACCOUNT": "prd-data-ldw-salesforce-1@prd-data-ldw-salesforce-1.iam.gserviceaccount.com","BI_SALESFORCE_GCP_KEY_FILE_PATH": "/home/farfetchbi-salesforce/gcs_keys/prd-data-ldw-salesforce-1.json","BI_RESOURCES_FOLDER_SALESFORCE": "/etc/airflow/resources/BI/BI-Salesforce/","CASE_BACKFILL_CALL_END_DATE": "2022-09-01","CASE_BACKFILL_CALL_START_DATE": "2022-03-17","CASE_BACKFILL_CHAT_END_DATE": "2021-08-17","CASE_BACKFILL_CHAT_START_DATE": "2019-01-01","CASE_BACKFILL_END_DATE": "2023-03-01","CASE_BACKFILL_START_DATE": "2019-01-01","CASE_BACKFILL_WO_AGG_END_DATE": "2023-03-01","CASE_BACKFILL_WO_AGG_START_DATE": "2019-01-01","BACKFILL_BACKLOG_START_DATE": "2021-06-06","BACKFILL_BACKLOG_END_DATE": "2021-06-08","STEP4_BACKFILL_START_DATE": "2019-01-01","STEP4_BACKFILL_END_DATE": "2022-01-03","CASE_BACKFILL_TYPE": "FULL","MAX_CONCURRENT_TASKS_RUNS": "16","MAX_CONCURRENT_TASKS_RUNS_DQ": "5","DQ_CONTEXT": "bi_operations","DQ_BQ_DEFINTITON_TABLE": "qa_scenario","DQ_BQ_EXECUTIONS_TABLE": "qa_execution","DQ_TESTS_PATH": "/tests/P0/","DQ_PARALLEL_TASKS": "15","DQ_PARALLEL_TASKS_MAIN_DAG": "5","WORKDAY_HTTP_CONNECTION": "WORKDAY_HTTP_CONNECTION","WORKDAY_HTTP_CONNECTION_TEMP": "WORKDAY_HTTP_CONNECTION_TEMP","WORKDAY_ENDPOINT": "/ccx/service/customreport2/farfetch/FF_WorkforceMng/Farfetch_Workforce_Planning_Team_Report_Integration_Planning_v2?format=json","WORKDAY_METHOD": "GET","WORKDAY_HEADERS": {"Content-Type": "application/json"},"DQ_AGENTQUALITY_TRIGGER_RULE": "all_done","DQ_CALL_TRIGGER_RULE": "all_done","DQ_DIMENSIONS_TRIGGER_RULE": "all_done","DQ_CHAT_TRIGGER_RULE": "all_done","DQ_FORECAST_TRIGGER_RULE": "all_done","DQ_STEP4_TRIGGER_RULE": "all_done","DQ_DIMCONTACTCARIIER_IGNORE_P0": "Y","DQ_PHONEORDERS_IGNORE_P0": "Y","DQ_DIM_IGNORE_P0": "Y","DQ_CALL_IGNORE_P0": "Y","DQ_CHAT_IGNORE_P0": "Y","DQ_FORECAST_IGNORE_P0": "Y","DQ_AGENT_IGNORE_P0": "Y","DQ_FORECAST_RC_IGNORE_P0": "Y","DQ_EMPLOYEE_STATUS_IGNORE_P0": "Y","DQ_SESSION_TIME_IGNORE_P0": "Y","BIOP_SE_CASE_BACKFILL_WO_AGG_START_DATE": "2019-01-01","BIOP_SE_CASE_BACKFILL_WO_AGG_END_DATE": "2022-04-07","BQ_PROJECT_PII": "prd-data-ldw-salesforcepii-1","BQ_DATASET_PII": "gold_read_pii","DQ_EMPLOYEESE_IGNORE_P0": "Y","BQ_DATASET_PROSCHEDULER": "proscheduler_data","BQ_DATASET_SILVER": "silver_read","DQ_CSP_IGNORE_P0": "Y","MAX_CONCURRENT_TASKS_CASE_BKF": "2","BQ_PROJECT_ODS": "prd-data-platform-ods-1","DQ_PROJECT_IGNORE_P0": "Y","BQ_PROJECT_CORE": "prd-data-ldw-core-1","BQ_DATASET_XPL": "xpl","MAX_CONCURRENT_TASKS_RUNS_PROJECT_DQ": "3","BQ_DATASET_ODS_STG": "ods_stg","BQ_DATASET_LDW_STG": "ldw_stg","BQ_DATASET_XPL_STG": "xpl_stg","BQ_PROJECT_LDW": "prd-data-ldw-salesforce-1","DQ_ODS_UIPATH_IGNORE_P0": "Y","DQ_LDW_UIPATH_IGNORE_P0": "Y","DQ_XPL_UIPATH_IGNORE_P0": "Y","UIPATH_LDW_NR_PROCESS_DAYS": "1","DQ_USERID_IGNORE_P0": "Y","DQ_CUSTOMER_CONVERSION_IGNORE_P0": "Y","MAX_CONCURRENT_DAG_RUNS_SALESFORCE_ODS_BACKFILL": "5","REQUEST_METHOD": "GET", "ENDPOINT_POS_MOV":"/ccx/service/customreport2/farfetch/FF_WorkforceMng/Farfetch___All_Position_Movements_-_Integration?Business_Processes%21WID=cd09beac446c11de98360015c5e6daf6!c24592468ed147b2ac6d0de4d699a7da!cd0dec66446c11de98360015c5e6daf6!ee38251b368a10000e1ca96918806639!cd09bb46446c11de98360015c5e6daf6!cd09b970446c11de98360015c5e6daf6!8fa06871cb4a4f96a6d3fb49d7448725!956972d0179342df82c26bb0781d9660!cd0dbe44446c11de98360015c5e6daf6!86de2b1b1d06100011853cddf5160000!cd0dcca4446c11de98360015c5e6daf6!cd9d9614118f4a5d9c87172fd5029eaf!cd0de25c446c11de98360015c5e6daf6!cd0a056a446c11de98360015c5e6daf6!cd0de176446c11de98360015c5e6daf6!cd0ddeb0446c11de98360015c5e6daf6&Transaction_Status%21WID=b90bc51be01d4ae99b603b02b073714d&Event_Completed_Date_On_or_Before=end_date-00%3A00&Event_Completed_Date_On_or_After=start_date-00%3A00&format=json","ENDPOINT_ALL_POS":"/ccx/service/customreport2/farfetch/FF_WorkforceMng/Farfetch___All_Positions_Ever___Integration?format=json","ENDPOINT_EMPLOYEES":"/ccx/service/customreport2/farfetch/FF_WorkforceMng/Farfetch_Workforce_Planning_Team_Report_Integration_Planning_v2?format=json"}'
)

## Iterate over subfolders and create a PBN object for each one of them
dags_folder = os.path.join(start_path, "dags")
resources_folder = os.path.join(start_path, "resources//bq_scripts")


## For each one of these folders, create an instance of the PBN class
for folder in os.listdir(dags_folder):
    if folder[:9] == "data-core":
        pbn = PBN(
            name=folder,
            dag_pbn_path=os.path.join(dags_folder, folder),
            resources_pbn_path=(
                os.path.join(resources_folder, folder, "etl")
                if folder == "data-core-serpa"
                else os.path.join(resources_folder, folder)
            ),
        )
        PBNs.append(pbn)

### CLASS INSTANCES SETUP ###

## Go into each of these PBNs and go over each file. Each one of them will be a DAG class instance. Add DAG to the PBN.
for pbn in PBNs:

    # For the MVP we're ignoring this one since the structure is a bit different from the rest.
    if pbn.name == "data-core-seprojectmanagement":
        continue

    # Iterate to get DAGs and Tasks
    for filename in os.listdir(pbn.dag_pbn_path):

        # Ignorint DQ and BAckfill DAGs for now
        if (
            filename.endswith(".py")
            and "-dq" not in filename
            and "-backfill" not in filename
        ):
            # Create DAG instance
            dag = Dag(name=filename)

            # Walk the python script and find assignation to variables and relevant tasks.
            # For this we use the ast module in python. More info here: https://docs.python.org/3/library/ast.html
            file_path = os.path.join(pbn.dag_pbn_path, filename)
            with open(file_path, "r") as file:
                tree = ast.parse(file.read(), filename=file_path)
            table_lists_definition = {}
            dataset_lists_definition = {}
            bigquery_parameters = {}

            for node in ast.walk(tree):
                # If is an assignation to variable
                if isinstance(node, ast.Assign):

                    for target in node.targets:
                        # If variable name is a string, store it in var_name
                        if isinstance(target, ast.Name):
                            var_name = target.id

                            regexp = re.compile(r".+?\[[\'\"](.+?)[\'\"]\]")
                            # If variable contains BQ_PROJECT or BQ_DATASET, we assume it is fetched from BI_OPERATIONS_SALSEFORCE_PARAMETERs.
                            # Here we're using regex to extract the key and then looking for this key in the PARAMETERS to get the final value attributed to the variable
                            if "BQ_PROJECT" in target.id or "BQ_DATASET" in target.id:
                                bigquery_parameters[var_name] = (
                                    AIRFLOW_VAR_BI_OPERATIONS_SALESFORCE_PARAMETERS[
                                        re.findall(regexp, ast.unparse(node.value))[0]
                                    ]
                                )

                            ## In caselog dag, the format is different. Hard code the dataset tables, else calculate it from files.
                            if dag.name in [
                                "data-core-secaselog-ldw.py",
                                "data-core-secaselog-xpl.py",
                            ]:
                                dataset_lists_definition["datasets"] = {
                                    "_dataset-1_": "ods",
                                    "_dataset-2_": "ods_stg",
                                    "_dataset-3_": "ldw",
                                    "_dataset-4_": "ldw_stg",
                                    "_dataset-5_": "ldw",
                                    "_dataset-6_": "xpl",
                                    "_dataset-7_": "xpl_stg",
                                    "_dataset-8_": "external",
                                    "_dataset-9_": "tmp",
                                    "_dataset-10_": "wfm",
                                    "_dataset-11_": "salesforce",
                                    "_dataset-12_": "data_quality",
                                    "_dataset-13_": "gold_raw",
                                    "_dataset-14_": "gold_read",
                                    "_dataset-15_": "gold_read",
                                    "_dataset-16_": "landing_zone",
                                    "_dataset-17_": "forecast",
                                    "_dataset-18_": "gold_read_pii",
                                    "_dataset-19_": "proscheduler_data",
                                    "_dataset-20_": "silver_read",
                                }

                            # Here wefetch the dataset_lists that will be passed to the DAG's tasks.
                            # For each ofne of the dataset_list and table_lists, a dictionary is created and stored in the dag instance
                            else:
                                if "dataset_list" in target.id:
                                    dataset_lists_definition[var_name] = {}

                                    for key, value in zip(
                                        node.value.keys, node.value.values
                                    ):
                                        # if value is a name (other variable), get it's value from bigquery_parameters
                                        if isinstance(value, ast.Name):
                                            dataset_lists_definition[var_name][
                                                key.s
                                            ] = bigquery_parameters[value.id]

                                        # if value is a constant (string), store it with value.value
                                        if isinstance(value, ast.Constant):
                                            dataset_lists_definition[var_name][
                                                key.s
                                            ] = value.value

                                        # if value is a subscript (variable["key"]), get the value from PAIRFLOW_VAR_BI_OPERATIONS_SALESFORCE_PARAMETERS
                                        if isinstance(value, ast.Subscript):
                                            dataset_lists_definition[var_name][
                                                key.s
                                            ] = AIRFLOW_VAR_BI_OPERATIONS_SALESFORCE_PARAMETERS[
                                                re.findall(
                                                    regexp,
                                                    ast.unparse(value),
                                                )[0]
                                            ]

                            # Table list a dict with string values, so we can simply use ast.literal_eval
                            if "table_list" in target.id:
                                table_lists_definition[var_name] = ast.literal_eval(
                                    node.value
                                )

                            # if it is calling a function and function name in 'run_dml_script' or 'run_raw_sql'or "run_copy_table_dml" we want to process it to get the destination and sources tables.
                            if (
                                isinstance(node, ast.Assign)
                                and isinstance(node.value, ast.Call)
                                and isinstance(node.value.func, ast.Name)
                                and (
                                    node.value.func.id == "run_dml_script"
                                    or node.value.func.id == "run_raw_sql"
                                    or node.value.func.id == "run_copy_table_dml"
                                )
                            ):
                                # Create task instance
                                task_name = target.id
                                task_function = node.value.func.id
                                task = Task(name=task_name, type=task_function)

                                parameters = {}

                                # for each argument passed to the function, create an attribute in the task instance.
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

                                # Aassign the DAG that has the task to the task instance
                                task.dag = dag

                                # If the task points to a file_name (which is the sql script), build the resource path
                                if hasattr(task, "file_name"):
                                    task.resource_path = os.path.join(
                                        pbn.resources_pbn_path,
                                        task.file_name.replace("/", "", 1),
                                    )
                                # Add task to the DAG's taks list
                                dag.add_task(task=task)

                                # Add task to the task lists
                                tasks.append(task)

            # Write the calculated parameters to the instance variables.
            dag.table_lists = table_lists_definition
            dag.dataset_lists = dataset_lists_definition
            dag.bq_parameters = bigquery_parameters

            # Add dag to the DAGs list
            dags.append(dag)

            # Add dag to the PBN dags
            pbn.add_dag(dag)

    # For each resource in the PBN, create a Resource instance and add it to the PBN ubstabce
    for filename in os.listdir(pbn.resources_pbn_path):
        if filename.endswith(".sql"):
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


# Assignt he resource for each one of the taks. From this resource assigned, define the destination table and the source tables.
for pbn in PBNs:
    for dag in pbn.dags:
        for task in dag.tasks:
            for resource in pbn.resources:
                if hasattr(task, "file_name"):
                    if resource.name == task.file_name.replace("/", "") or (
                        "etl" + resource.name
                    ) == task.file_name.replace("/", ""):
                        task.resource = resource
            task.define_dest_table()
            task.define_source_tables()


# Create Network Instance
net = Network(
    height="750px",
    width="100%",
    bgcolor="#222222",
    font_color="white",
    select_menu=True,
    filter_menu=True,
)

sources_added = []
targets_added = []

# Flag do define if we want a global view (source and target nodes linked regardless of the DAG) or a DAG view (one tree per dag, tables might appear separetly in multiple dags)
global_view = False

if global_view:
    for pbn in PBNs:
        for dag in pbn.dags:
            for task in dag.tasks:
                target = task.dest_table
                sources = task.source_tables

                if target not in targets_added:
                    net.add_node(target, target, title=target, group=dag)
                    targets_added.append(target)

                for source in sources:
                    if source not in sources_added:
                        net.add_node(source, source, title=source, group=dag)
                        sources_added.append(source)

                    net.add_edge(source, target)

if global_view is False:
    for pbn in PBNs:
        for dag in pbn.dags:
            for task in dag.tasks:
                target = task.dest_table
                sources = task.source_tables

                if target not in targets_added:
                    title_string = f"<b>PBN:</b> {pbn.name}<br><b>DAG:</b> {dag.name}<br><b>Table:</b> {target}"
                    target_id = pbn.name + "-" + target
                    net.add_node(
                        n_id=target_id,
                        label=target,
                        title=title_string,
                        group=dag.name,
                        dag=dag.name,
                    )
                    targets_added.append(target_id)

                for source in sources:
                    group = "None"
                    title_pbn = "None"
                    for t in tasks:
                        if source == t.dest_table:
                            group = t.dag.name
                            title_pbn = t.dag.pbn.name
                            break

                    title_string = f"<b>PBN:</b> {title_pbn}<br><b>DAG:</b> {group}<br><b>Table:</b> {source}"
                    if source not in sources_added:
                        source_id = pbn.name + "-" + source
                        net.add_node(
                            n_id=source_id,
                            label=source,
                            title=title_string,
                            group=group,
                            dag=dag.name,
                        )
                        sources_added.append(source_id)

                    net.add_edge(source_id, target_id)

net.set_options(
    """ const options = {
  "nodes": {
    "borderWidth": 1,
    "borderWidthSelected": 2,
    "opacity": 1,
    "size": 25
  },
  "edges": {
    "arrows": {
      "to": {
        "enabled": true
      }
    },
    "color": {
      "inherit": true
    },
    "selfReferenceSize": null,
    "selfReference": {
      "angle": 0.7853981633974483
    },
    "smooth": false
  },
  "layout": {
    "hierarchical": {
      "enabled": true
    }
  }
} """
)


## Function to turn off physics in the Network once it's stabilized. This is to allow movement of the nodes without them comming back.
def add_physics_stop_to_html(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
        content = file.read()

    # Search for the stabilizationIterationsDone event and insert the network.setOptions line
    pattern = r'(network.once\("stabilizationIterationsDone", function\(\) {)'
    replacement = r"\1\n\t\t\t\t\t\t  // Disable the physics after stabilization is done.\n\t\t\t\t\t\t  network.setOptions({ physics: false });"

    new_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    # Write the modified content back to the file
    with open(filepath, "w", encoding="utf-8") as file:
        file.write(new_content)


net.write_html("test.html", notebook=False)
add_physics_stop_to_html("test.html")

## Todo
## 1. Organize Script
## 2. consider tasks that call run_copy_table_dml function
