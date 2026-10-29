from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "velib",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

JARS = (
    "/opt/spark/jars-extra/delta-spark_2.12-3.2.0.jar,"
    "/opt/spark/jars-extra/delta-storage-3.2.0.jar,"
    "/opt/spark/jars-extra/hadoop-aws-3.3.4.jar,"
    "/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar,"
    "/opt/spark/jars-extra/postgresql-42.7.4.jar"
)

SPARK_MOUNTS = [
    {"source": "velib-spark-apps", "target": "/opt/spark-apps", "type": "volume"},
    {"source": "velib-spark-jars", "target": "/opt/spark/jars-extra", "type": "volume"},
]

DBT_MOUNTS = [
    {"source": "velib-dbt-project", "target": "/usr/app/velib", "type": "volume"},
    {"source": "velib-dbt-profiles", "target": "/root/.dbt", "type": "volume"},
]

with DAG(
    dag_id="velib_pipeline",
    description="Silver -> export Postgres -> dbt run -> dbt test",
    default_args=default_args,
    start_date=datetime(2026, 10, 29),
    schedule_interval="@hourly",
    catchup=False,
    tags=["velib", "medallion"],
) as dag:

    run_silver = DockerOperator(
        task_id="run_silver",
        image="apache/spark:3.5.0",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="velib-platform_default",
        mounts=SPARK_MOUNTS,
        command=(
            "/opt/spark/bin/spark-submit --master local[*] "
            f"--jars {JARS} "
            "/opt/spark-apps/silver_job.py"
        ),
        mount_tmp_dir=False,
    )

    export_pg = DockerOperator(
        task_id="export_to_postgres",
        image="apache/spark:3.5.0",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="velib-platform_default",
        mounts=SPARK_MOUNTS,
        command=(
            "/opt/spark/bin/spark-submit --master local[*] "
            f"--jars {JARS} "
            "/opt/spark-apps/export_to_postgres.py"
        ),
        mount_tmp_dir=False,
    )

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="python:3.11-slim",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="velib-platform_default",
        mounts=DBT_MOUNTS,
        command=(
            "/bin/bash -c 'pip install --quiet dbt-postgres && "
            "dbt run --project-dir /usr/app/velib --profiles-dir /root/.dbt'"
        ),
        mount_tmp_dir=False,
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image="python:3.11-slim",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="velib-platform_default",
        mounts=DBT_MOUNTS,
        command=(
            "/bin/bash -c 'pip install --quiet dbt-postgres && "
            "dbt test --project-dir /usr/app/velib --profiles-dir /root/.dbt'"
        ),
        mount_tmp_dir=False,
    )

    run_silver >> export_pg >> dbt_run >> dbt_test