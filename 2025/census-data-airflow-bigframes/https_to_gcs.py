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

from airflow.providers.google.cloud.transfers import http_to_gcs


default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime.datetime(2018, 1, 1),
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    "composer_sample_simple_greeting",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:
    http_to_gcs_task = http_to_gcs.HttpToGCSOperator(
        task_id="http_to_gcs_task",
        endpoint="https://www2.census.gov/programs-surveys/popest/datasets/2020-2023/counties/asrh/cc-est2023-agesex-all.csv",
        method="GET",
        dest_gcs="gs://us-central1-bigframes-orche-b70f2a52-bucket/data/us-census/",
    )