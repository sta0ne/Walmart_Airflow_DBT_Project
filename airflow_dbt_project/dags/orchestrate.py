from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time
import pendulum



@dag(
        dag_id="orchestrate",
        schedule="0 11 * * *",
        catchup=False,
        start_date=pendulum.datetime(year=2026, month=9, day=14, tz="Asia/Kolkata")
)
def orchestrate():

    @task
    def ingest_cdc():
        ws = WorkspaceClient(
        host="your_databricks_host",
        token="your_databricks_token")

        job_trigger = ws.jobs.run_now(job_id = "your_databricks_job_id")

        while True:
            job_status = ws.jobs.get_run(
                run_id=job_trigger.run_id
            )

            
            
            if job_status.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_status.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully.")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_status.state.result_state}")

            time.sleep(5)  # Wait for 10 seconds before checking the status again

        return "CDC ingestion completed successfully."

        


    @task.bash
    def clean_target():
        return " rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        return (
            "dbt source freshness "
            "--project-dir /opt/airflow/walmart_project "
            "--profiles-dir /opt/airflow/walmart_project"
        )

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_t"
    )

    silver_technical_test = BashOperator(
        task_id="silver_technical_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_t"
    )
    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_b"
    )

    silver_business_test = BashOperator(
        task_id="silver_business_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_b"
    )

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select gold/ephemeral"
    )

    gold_dimension = BashOperator(
        task_id="gold_dimension",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt snapshot"
    )

    gold_facts = BashOperator(
        task_id="gold_facts",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select gold/fact"
    )
    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_test >> silver_business >> silver_business_test >> gold_ephemeral >> gold_dimension >> gold_facts


orchestrate_dag = orchestrate()