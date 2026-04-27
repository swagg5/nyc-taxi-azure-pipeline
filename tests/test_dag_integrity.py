# tests/test_dag_integrity.py
# Airflow DAG integrity tests
# Verify structure, dependencies, and schedule without executing tasks
# These run in CI on every PR — catch DAG errors before deployment

import pytest
from airflow.models import DagBag


@pytest.fixture(scope='module')
def dagbag():
    '''Load all DAGs from the airflow/ directory'''
    return DagBag(dag_folder='airflow/', include_examples=False)


class TestDagLoading:
    '''Tests that all DAGs load without errors'''

    def test_no_import_errors(self, dagbag):
        '''No DAG should have import errors'''
        assert len(dagbag.import_errors) == 0, \
            f'DAG import errors: {dagbag.import_errors}'

    def test_all_three_dags_present(self, dagbag):
        '''All three pipeline DAGs must be present'''
        expected = {
            'dag_bronze_ingest',
            'dag_silver_transform',
            'dag_gold_build'
        }
        actual = set(dagbag.dag_ids)
        assert expected.issubset(actual), \
            f'Missing DAGs: {expected - actual}'


class TestBronzeDAG:
    '''Tests for dag_bronze_ingest structure'''

    def test_task_count(self, dagbag):
        dag = dagbag.get_dag('dag_bronze_ingest')
        assert len(dag.tasks) == 2, \
            f'Bronze DAG should have 2 tasks, has {len(dag.tasks)}'

    def test_task_ids(self, dagbag):
        dag = dagbag.get_dag('dag_bronze_ingest')
        task_ids = {t.task_id for t in dag.tasks}
        assert 'trigger_adf_pipeline' in task_ids
        assert 'verify_bronze_files' in task_ids

    def test_dependency_order(self, dagbag):
        '''trigger must run before verify'''
        dag = dagbag.get_dag('dag_bronze_ingest')
        trigger = dag.get_task('trigger_adf_pipeline')
        verify  = dag.get_task('verify_bronze_files')
        assert verify.task_id in [t.task_id for t in trigger.downstream_list]

    def test_schedule(self, dagbag):
        dag = dagbag.get_dag('dag_bronze_ingest')
        assert dag.schedule_interval == '@monthly' or dag.schedule_interval == '0 0 1 * *'

    def test_catchup_false(self, dagbag):
        dag = dagbag.get_dag('dag_bronze_ingest')
        assert dag.catchup == False, 'catchup must be False to prevent backfill floods'

    def test_retries_configured(self, dagbag):
        dag = dagbag.get_dag('dag_bronze_ingest')
        for task in dag.tasks:
            assert task.retries >= 1, \
                f'Task {task.task_id} must have at least 1 retry configured'


class TestGoldDAG:
    '''Tests for dag_gold_build parallel structure'''

    def test_task_count(self, dagbag):
        dag = dagbag.get_dag('dag_gold_build')
        assert len(dag.tasks) == 4

    def test_parallel_gold_tasks(self, dagbag):
        '''run_gold_metrics and run_gold_dim should run in parallel'''
        dag = dagbag.get_dag('dag_gold_build')
        wait  = dag.get_task('wait_for_silver')
        downstream_ids = {t.task_id for t in wait.downstream_list}
        assert 'run_gold_metrics' in downstream_ids
        assert 'run_gold_dim'     in downstream_ids

    def test_verify_runs_after_both_gold_tasks(self, dagbag):
        '''verify_gold_output must wait for both metrics and dim'''
        dag    = dagbag.get_dag('dag_gold_build')
        verify = dag.get_task('verify_gold_output')
        upstream_ids = {t.task_id for t in verify.upstream_list}
        assert 'run_gold_metrics' in upstream_ids
        assert 'run_gold_dim'     in upstream_ids