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

AIRFLOW_VAR_BI_OPERATIONS_SALESFORCE_PARAMETERS = json.loads(
    '{"PROJECT_NAME": "BI-Salesforce","BI_SALESFORCE_DEBUG_MODE": 0,"ERROR_MAIL_OPS": "dl-pt-ftech-bi-operations@farfetch.com","ERROR_MAIL_DQ_P5": "joaocerca.santos@farfetch.com,miguel.duarte@farfetch.com","GCP_BQ_CON_ID": "prd-data-ldw-salesforce-1","BQ_PROJECT_WB_ID": "bigquery-analytics-workbench","BQ_PROJECT_WFOPS_ID": "prd-data-teams-wfops-1","BQ_PROJECT_WB_VIEWS": "bigquery-analytics-workbench","BQ_DATASET_WFOPS_LZ": "landing_zone","BQ_DATASET_STAGING": "gold_raw","BQ_DATASET_EXTERNAL": "external","BQ_DATASET_SALESFORCE": "salesforce","BQ_DATASET_WFM": "wfm","BQ_DATASET_DW": "gold_read","BQ_DATASET_WB": "gold_read","BQ_DATASET_ODS": "ods","BQ_DATASET_DQ": "data_quality","BQ_DATASET_WFOPS": "forecast","BQ_DATASET_LDW": "ldw","FORECAST_BACKFILL_START_DATE": "2021-07-22","FORECAST_BACKFILL_END_DATE": "2021-09-01","BI_SALESFORCE_GCP_PROJECT": "prd-data-ldw-salesforce-1","BQ_SALESFORCE_SERVICE_ACCOUNT": "prd-data-ldw-salesforce-1@prd-data-ldw-salesforce-1.iam.gserviceaccount.com","BI_SALESFORCE_GCP_KEY_FILE_PATH": "/home/farfetchbi-salesforce/gcs_keys/prd-data-ldw-salesforce-1.json","BI_RESOURCES_FOLDER_SALESFORCE": "/etc/airflow/resources/BI/BI-Salesforce/","CASE_BACKFILL_CALL_END_DATE": "2022-09-01","CASE_BACKFILL_CALL_START_DATE": "2022-03-17","CASE_BACKFILL_CHAT_END_DATE": "2021-08-17","CASE_BACKFILL_CHAT_START_DATE": "2019-01-01","CASE_BACKFILL_END_DATE": "2023-03-01","CASE_BACKFILL_START_DATE": "2019-01-01","CASE_BACKFILL_WO_AGG_END_DATE": "2023-03-01","CASE_BACKFILL_WO_AGG_START_DATE": "2019-01-01","BACKFILL_BACKLOG_START_DATE": "2021-06-06","BACKFILL_BACKLOG_END_DATE": "2021-06-08","STEP4_BACKFILL_START_DATE": "2019-01-01","STEP4_BACKFILL_END_DATE": "2022-01-03","CASE_BACKFILL_TYPE": "FULL","MAX_CONCURRENT_TASKS_RUNS": "16","MAX_CONCURRENT_TASKS_RUNS_DQ": "5","DQ_CONTEXT": "bi_operations","DQ_BQ_DEFINTITON_TABLE": "qa_scenario","DQ_BQ_EXECUTIONS_TABLE": "qa_execution","DQ_TESTS_PATH": "/tests/P0/","DQ_PARALLEL_TASKS": "15","DQ_PARALLEL_TASKS_MAIN_DAG": "5","WORKDAY_HTTP_CONNECTION": "WORKDAY_HTTP_CONNECTION","WORKDAY_HTTP_CONNECTION_TEMP": "WORKDAY_HTTP_CONNECTION_TEMP","WORKDAY_ENDPOINT": "/ccx/service/customreport2/farfetch/FF_WorkforceMng/Farfetch_Workforce_Planning_Team_Report_Integration_Planning_v2?format=json","WORKDAY_METHOD": "GET","WORKDAY_HEADERS": {"Content-Type": "application/json"},"DQ_AGENTQUALITY_TRIGGER_RULE": "all_done","DQ_CALL_TRIGGER_RULE": "all_done","DQ_DIMENSIONS_TRIGGER_RULE": "all_done","DQ_CHAT_TRIGGER_RULE": "all_done","DQ_FORECAST_TRIGGER_RULE": "all_done","DQ_STEP4_TRIGGER_RULE": "all_done","DQ_DIMCONTACTCARIIER_IGNORE_P0": "Y","DQ_PHONEORDERS_IGNORE_P0": "Y","DQ_DIM_IGNORE_P0": "Y","DQ_CALL_IGNORE_P0": "Y","DQ_CHAT_IGNORE_P0": "Y","DQ_FORECAST_IGNORE_P0": "Y","DQ_AGENT_IGNORE_P0": "Y","DQ_FORECAST_RC_IGNORE_P0": "Y","DQ_EMPLOYEE_STATUS_IGNORE_P0": "Y","DQ_SESSION_TIME_IGNORE_P0": "Y","BIOP_SE_CASE_BACKFILL_WO_AGG_START_DATE": "2019-01-01","BIOP_SE_CASE_BACKFILL_WO_AGG_END_DATE": "2022-04-07","BQ_PROJECT_PII": "prd-data-ldw-salesforcepii-1","BQ_DATASET_PII": "gold_read_pii","DQ_EMPLOYEESE_IGNORE_P0": "Y","BQ_DATASET_PROSCHEDULER": "proscheduler_data","BQ_DATASET_SILVER": "silver_read","DQ_CSP_IGNORE_P0": "Y","MAX_CONCURRENT_TASKS_CASE_BKF": "2","BQ_PROJECT_ODS": "prd-data-platform-ods-1","DQ_PROJECT_IGNORE_P0": "Y","BQ_PROJECT_CORE": "prd-data-ldw-core-1","BQ_DATASET_XPL": "xpl","MAX_CONCURRENT_TASKS_RUNS_PROJECT_DQ": "3","BQ_DATASET_ODS_STG": "ods_stg","BQ_DATASET_LDW_STG": "ldw_stg","BQ_DATASET_XPL_STG": "xpl_stg","BQ_PROJECT_LDW": "prd-data-ldw-salesforce-1","DQ_ODS_UIPATH_IGNORE_P0": "Y","DQ_LDW_UIPATH_IGNORE_P0": "Y","DQ_XPL_UIPATH_IGNORE_P0": "Y","UIPATH_LDW_NR_PROCESS_DAYS": "1","DQ_USERID_IGNORE_P0": "Y","DQ_CUSTOMER_CONVERSION_IGNORE_P0": "Y","MAX_CONCURRENT_DAG_RUNS_SALESFORCE_ODS_BACKFILL": "5","REQUEST_METHOD": "GET", "ENDPOINT_POS_MOV":"/ccx/service/customreport2/farfetch/FF_WorkforceMng/Farfetch___All_Position_Movements_-_Integration?Business_Processes%21WID=cd09beac446c11de98360015c5e6daf6!c24592468ed147b2ac6d0de4d699a7da!cd0dec66446c11de98360015c5e6daf6!ee38251b368a10000e1ca96918806639!cd09bb46446c11de98360015c5e6daf6!cd09b970446c11de98360015c5e6daf6!8fa06871cb4a4f96a6d3fb49d7448725!956972d0179342df82c26bb0781d9660!cd0dbe44446c11de98360015c5e6daf6!86de2b1b1d06100011853cddf5160000!cd0dcca4446c11de98360015c5e6daf6!cd9d9614118f4a5d9c87172fd5029eaf!cd0de25c446c11de98360015c5e6daf6!cd0a056a446c11de98360015c5e6daf6!cd0de176446c11de98360015c5e6daf6!cd0ddeb0446c11de98360015c5e6daf6&Transaction_Status%21WID=b90bc51be01d4ae99b603b02b073714d&Event_Completed_Date_On_or_Before=end_date-00%3A00&Event_Completed_Date_On_or_After=start_date-00%3A00&format=json","ENDPOINT_ALL_POS":"/ccx/service/customreport2/farfetch/FF_WorkforceMng/Farfetch___All_Positions_Ever___Integration?format=json","ENDPOINT_EMPLOYEES":"/ccx/service/customreport2/farfetch/FF_WorkforceMng/Farfetch_Workforce_Planning_Team_Report_Integration_Planning_v2?format=json"}'
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
            resources_pbn_path=(
                os.path.join(resources_folder, folder, "etl")
                if folder == "data-core-serpa"
                else os.path.join(resources_folder, folder)
            ),
        )
        PBNs.append(pbn)

## Go into each of these PBNs and go over each file. Each one of them will be a DAG class instance. Add DAG to the PBN.
for pbn in PBNs:

    if pbn.name == "data-core-seprojectmanagement":
        continue

    # Iterate to get DAGs and Tasks
    for filename in os.listdir(pbn.dag_pbn_path):
        if (
            filename.endswith(".py")
            and "-dq" not in filename
            and "-backfill" not in filename
        ):
            dag = Dag(name=filename)
            file_path = os.path.join(pbn.dag_pbn_path, filename)

            with open(file_path, "r") as file:
                tree = ast.parse(file.read(), filename=file_path)
            table_lists_definition = {}
            dataset_lists_definition = {}
            bigquery_parameters = {}

            # Let's do a MVP without Datasets and Projects because that is more complicated.
            regexp = re.compile(r"table_list(_[^,]+?\s|\s)=\s*({.+?})", re.S)

            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    for target in node.targets:

                        if isinstance(target, ast.Name):
                            var_name = target.id

                            regexp = re.compile(r".+?\[[\'\"](.+?)[\'\"]\]")
                            if "BQ_PROJECT" in target.id or "BQ_DATASET" in target.id:
                                bigquery_parameters[var_name] = (
                                    AIRFLOW_VAR_BI_OPERATIONS_SALESFORCE_PARAMETERS[
                                        re.findall(regexp, ast.unparse(node.value))[0]
                                    ]
                                )
                            ## in caselog dag, the format is different. Hard code the dataset tables, else calculate it from files.
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

                            else:
                                if "dataset_list" in target.id:
                                    # dataset_lists_definition[var_name] = {
                                    #     k.s: bigquery_parameters[v.id]
                                    #     for k, v in zip(node.value.keys, node.value.values)
                                    # }
                                    dataset_lists_definition[var_name] = {}

                                    for key, value in zip(
                                        node.value.keys, node.value.values
                                    ):
                                        if isinstance(value, ast.Name):
                                            dataset_lists_definition[var_name][
                                                key.s
                                            ] = bigquery_parameters[value.id]

                                        if isinstance(value, ast.Constant):
                                            dataset_lists_definition[var_name][
                                                key.s
                                            ] = value.value

                                        if isinstance(value, ast.Subscript):
                                            dataset_lists_definition[var_name][
                                                key.s
                                            ] = AIRFLOW_VAR_BI_OPERATIONS_SALESFORCE_PARAMETERS[
                                                re.findall(
                                                    regexp,
                                                    ast.unparse(value),
                                                )[0]
                                            ]

                            if "table_list" in target.id:
                                table_lists_definition[var_name] = ast.literal_eval(
                                    node.value
                                )
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
                                task.dag = dag

                                task.resource = os.path.join(
                                    pbn.resources_pbn_path,
                                    task.file_name.replace("/", "", 1),
                                )
                                dag.add_task(task=task)
                                tasks.append(task)

            dag.table_lists = table_lists_definition
            dag.dataset_lists = dataset_lists_definition
            dag.bq_parameters = bigquery_parameters
            dags.append(dag)
            pbn.add_dag(dag)

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

for pbn in PBNs:
    for dag in pbn.dags:
        for task in dag.tasks:
            for resource in pbn.resources:
                if resource.name == task.file_name.replace("/", "") or (
                    "etl" + resource.name
                ) == task.file_name.replace("/", ""):
                    task.resource = resource


for task in tasks:
    task.define_dest_table()
    task.resource.dest_table = task.dest_table
    task.define_source_tables()


net = Network(
    height="750px",
    width="100%",
    bgcolor="#222222",
    font_color="white",
    select_menu=True,
    filter_menu=True,
)
# net.barnes_hut()

sources_added = []
targets_added = []

global_view = False

if global_view:
    for pbn in PBNs:
        for dag in pbn.dags:
            for task in dag.tasks:
                target = task.dest_table
                sources = task.source_tables

                if target not in targets_added:
                    net.add_node(target, target, title=target, group=1)
                    targets_added.append(target)

                for source in sources:
                    if source not in sources_added:
                        net.add_node(source, source, title=source, group=1)
                        sources_added.append(source)

                    net.add_edge(source, target)

if global_view is False:
    for pbn in PBNs:
        for dag in pbn.dags:
            for task in dag.tasks:
                target = task.dest_table
                sources = task.source_tables

                if target not in targets_added:
                    title_string = f"<b>PBN:</b> {pbn.name}<br><b>DAG:</b> {dag.name}"
                    target_id = pbn.name + "-" + target
                    net.add_node(
                        n_id=target_id,
                        label=target,
                        title=title_string,
                        group=dag.name,
                    )
                    targets_added.append(target_id)

                for source in sources:
                    group = "None"
                    title_pbn = "None"
                    for new_task in tasks:
                        if source == new_task.dest_table:
                            group = new_task.dag.name
                            title_pbn = new_task.dag.pbn.name
                            break

                    title_string = f"<b>PBN:</b> {title_pbn}<br><b>DAG:</b> {group}"
                    if source not in sources_added:
                        source_id = pbn.name + "-" + source
                        net.add_node(
                            n_id=source_id,
                            label=source,
                            title=title_string,
                            group=group,
                        )
                        sources_added.append(source_id)

                    net.add_edge(source_id, target_id)

# net.show_buttons()


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
  },
  "physics": {
    "hierarchicalRepulsion": {
      "centralGravity": 0,
      "avoidOverlap": null
    },
    "minVelocity": 0.75,
    "solver": "hierarchicalRepulsion"
  }
} """
)

net.show("test.html", notebook=False)




## Todo 
## 1. Organize Script
## 2. consider tasks that call run_copy_table_dml function