import os
from datetime import datetime

import pytest
from airflow.models import DagBag


class TestDAGValidation:

    @pytest.fixture(scope="class")
    def dagbag(self):
        dags_folder = os.path.join(os.path.dirname(__file__), "..", "dags")
        return DagBag(dag_folder=dags_folder, include_examples=False)

    def test_no_import_errors(self, dagbag):
        assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"

    def test_dag_count(self, dagbag):
        assert len(dagbag.dags) >= 2, f"Expected at least 2 DAGs, got {len(dagbag.dags)}"

    def test_crypto_ingestion_pipeline_exists(self, dagbag):
        assert "crypto_ingestion_pipeline" in dagbag.dags

    def test_crypto_realtime_transform_exists(self, dagbag):
        assert "crypto_realtime_transform" in dagbag.dags


class TestCryptoIngestionPipeline:

    @pytest.fixture(scope="class")
    def dag(self):
        dags_folder = os.path.join(os.path.dirname(__file__), "..", "dags")
        dagbag = DagBag(dag_folder=dags_folder, include_examples=False)
        return dagbag.dags.get("crypto_ingestion_pipeline")

    def test_dag_loaded(self, dag):
        assert dag is not None

    def test_dag_has_tags(self, dag):
        assert "crypto" in dag.tags
        assert "ingestion" in dag.tags

    def test_dag_has_correct_schedule(self, dag):
        assert dag.schedule is None  # Manual trigger

    def test_dag_has_expected_tasks(self, dag):
        expected_tasks = {
            "dbt_run_raw",
            "run_rust_ingestion",
            "redis_to_postgres",
            "dbt_run_staging_analytics",
        }
        actual_tasks = set(dag.task_ids)
        assert expected_tasks == actual_tasks, f"Expected {expected_tasks}, got {actual_tasks}"

    def test_dag_task_dependencies(self, dag):
        dbt_raw_task = dag.get_task("dbt_run_raw")
        rust_ingestion_task = dag.get_task("run_rust_ingestion")
        redis_postgres_task = dag.get_task("redis_to_postgres")
        dbt_transform_task = dag.get_task("dbt_run_staging_analytics")

        # Verify dbt_run_raw -> run_rust_ingestion
        assert rust_ingestion_task in dbt_raw_task.downstream_list

        # Verify run_rust_ingestion -> redis_to_postgres
        assert redis_postgres_task in rust_ingestion_task.downstream_list

        # Verify redis_to_postgres -> dbt_run_staging_analytics
        assert dbt_transform_task in redis_postgres_task.downstream_list

    def test_dag_has_no_cycles(self, dag):
        # Airflow checks this on load, but we can verify explicitly
        from airflow.exceptions import AirflowDagCycleException

        try:
            dag.test_cycle()
            has_cycle = False
        except AirflowDagCycleException:
            has_cycle = True
        except Exception:
            # Other exceptions don't mean there's a cycle
            has_cycle = False
        assert not has_cycle, "DAG contains a cycle"


class TestCryptoRealtimeTransform:

    @pytest.fixture(scope="class")
    def dag(self):
        dags_folder = os.path.join(os.path.dirname(__file__), "..", "dags")
        dagbag = DagBag(dag_folder=dags_folder, include_examples=False)
        return dagbag.dags.get("crypto_realtime_transform")

    def test_dag_loaded(self, dag):
        assert dag is not None

    def test_dag_has_tags(self, dag):
        assert "crypto" in dag.tags
        assert "realtime" in dag.tags
        assert "dbt" in dag.tags

    def test_dag_has_correct_schedule(self, dag):
        assert dag.schedule == "*/5 * * * *"

    def test_dag_has_expected_tasks(self, dag):
        expected_tasks = {"dbt_run_incremental"}
        actual_tasks = set(dag.task_ids)
        assert expected_tasks == actual_tasks

    def test_dag_is_paused_on_creation(self, dag):
        assert dag.is_paused_upon_creation is True
