"""
My new doc
"""

import textwrap
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator

from airflow.sdk import DAG

with DAG(
    dag_id='tutorial',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # "queue": 'bash_queue',
        # "pool": 'backfill',
        # "priority_weight": 10,
        # "end_date": datetime(2026,1, 1),
        # "wait_for_downstream": False,
        # "execution_timeout": timedelta(seconds=300),
        # "on_failure_callback": None,
        # "on_success_callback": None,
        # "on_retry_callback": None,
        # "sla_miss_callback":None,
        # "on_skipped_callback": None,
        # "trigger_rule": 'all_success',
    },
    description="A simple tutorial DAG",
    schedule=timedelta(minutes=5),
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    t1 = BashOperator(
        task_id="task_1",
        bash_command="date",
    )

    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        retries=3,
        bash_command="sleep 5",
    )

    t1.doc_md = textwrap.dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](https://imgs.xkcd.com/comics/fixing_problems.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
        """
    )

    dag.doc_md = __doc__


    templated_command = textwrap.dedent(
        """
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7) }}"
        {% endfor %}
        """
    )

    t3 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=templated_command,
    )

    t1 >> [t2, t3]
