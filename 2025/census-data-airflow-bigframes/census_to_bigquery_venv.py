# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Tested on Cloud Composer 3
#
# For local development:
# pip install 'apache-airflow[google]==2.10.5'


import datetime

from airflow import models
from airflow.operators import bash
from airflow.operators.python import (
    PythonVirtualenvOperator,
)


default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime.datetime(2025, 6, 30),
}

GCS_LOCATION = "gs://us-central1-bigframes-orche-b70f2a52-bucket/data/us-census/cc-est2023-agesex-all.csv"

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    "census_from_http_to_gcs_once",
    schedule_interval="@once",
    default_args=default_dag_args,
) as dag:
    download = bash.BashOperator(
        task_id="download",
        bash_command="wget https://www2.census.gov/programs-surveys/popest/datasets/2020-2023/counties/asrh/cc-est2023-agesex-all.csv",
    )
    upload = bash.BashOperator(
        task_id="upload",
        bash_command=f"gcloud storage cp cc-est2023-agesex-all.csv {GCS_LOCATION}",
    )

    def callable_virtualenv():
        """
        Example function that will be performed in a virtual environment.

        Importing at the module level ensures that it will not attempt to import the
        library before it is installed.
        """
        import bigframes.pandas as bpd

        # Prevent the operator from accidentally downloading too many rows to
        # the client-side.
        bpd.options.compute.maximum_result_rows = 1000

        # TODO: read csv using bigquery engine
        # TODO: any sort of processing / cleanup?
        # TODO: some data validations (after cache())
        # TODO: write to destination table

    bf_to_gbq = PythonVirtualenvOperator(
        task_id="bf_to_gbq",
        python_callable=callable_virtualenv,
        requirements=["bigframes==2.9.0"],
        system_site_packages=False,
    )


    download >> upload >> bf_to_gbq
