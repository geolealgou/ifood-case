%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings as Job
from variables import WORKSPACE, WAREHOUSE_NAME

# Variáveis
workspace = WORKSPACE
warehouse_name = WAREHOUSE_NAME

w = WorkspaceClient()


def get_warehouse_id_by_name(warehouse_name: str) -> str:
    """
    Busca o ID do SQL Warehouse pelo nome no workspace atual.
    """
    warehouses = list(w.warehouses.list())

    for warehouse in warehouses:
        if warehouse.name == warehouse_name:
            return warehouse.id

    available = [warehouse.name for warehouse in warehouses]

    raise ValueError(
        f"Warehouse '{warehouse_name}' não encontrado. "
        f"Warehouses disponíveis: {available}"
    )


warehouse_id = get_warehouse_id_by_name(warehouse_name)

ETL = Job.from_dict(
    {
        "name": "ETL",
        "tasks": [
            {
                "task_key": "create_table",
                "sql_task": {
                    "file": {
                        "path": f"/Workspace/{workspace}/ifood-case/src/00_create_table.sql",
                        "source": "WORKSPACE",
                    },
                    "warehouse_id": warehouse_id,
                },
            },
            {
                "task_key": "ingestion",
                "depends_on": [{"task_key": "create_table"}],
                "spark_python_task": {
                    "python_file": f"/Workspace/{workspace}/ifood-case/src/01_ingestion.py",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "bronze_to_silver",
                "depends_on": [{"task_key": "ingestion"}],
                "spark_python_task": {
                    "python_file": f"/Workspace/{workspace}/ifood-case/src/02_bronze_to_silver.py",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "silver_to_gold",
                "depends_on": [{"task_key": "bronze_to_silver"}],
                "spark_python_task": {
                    "python_file": f"/Workspace/{workspace}/ifood-case/src/04_silver_to_gold.py",
                },
                "environment_key": "Default",
            },
        ],
        "tags": {
            "dominio": "etl_frota_taxi",
            "execucao": "mensal",
        },
        "queue": {
            "enabled": True,
        },
        "environments": [
            {
                "environment_key": "Default",
                "spec": {
                    "environment_version": "5",
                },
            },
        ],
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
)

# Cria um novo job
created_job = w.jobs.create(**ETL.as_shallow_dict())

print(f"Job criado com sucesso. Job ID: {created_job.job_id}")