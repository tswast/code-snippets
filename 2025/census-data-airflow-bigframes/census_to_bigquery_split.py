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

"""
An example DAG for loading data from the US Census using BigQuery DataFrames
(aka bigframes). This DAG uses PythonVirtualenvOperator for environments where
bigframes can't be installed for use from PythonOperator.

I have tested this DAG on Cloud Composer 3 with Apache Airflow 2.10.5.

For local development:

    pip install 'apache-airflow[google]==2.10.5' bigframes
"""

import datetime

from airflow import models
from airflow.operators import bash
from airflow.operators.python import (
    PythonOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator


default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime.datetime(2025, 6, 30),
}

GCS_LOCATION = "gs://us-central1-bigframes-orche-0186cdcf-bucket/data/us-census/cc-est2024-agesex-all.csv"

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    "census_from_http_to_bigquery_split_once",
    schedule_interval="@once",
    default_args=default_dag_args,
) as dag:
    download_upload = bash.BashOperator(
        task_id="download_upload",
        # See
        # https://www.census.gov/data/tables/time-series/demo/popest/2020s-counties-detail.html
        # for file paths and methodologies.
        bash_command=f"""
        wget https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/asrh/cc-est2024-agesex-all.csv -P ~;
        gcloud storage cp ~/cc-est2024-agesex-all.csv {GCS_LOCATION}
        """,
    )

    def preprocess(task_instance):
        import datetime

        import bigframes.pandas as bpd

        GCS_LOCATION = "gs://us-central1-bigframes-orche-0186cdcf-bucket/data/us-census/cc-est2024-agesex-all.csv"

        bpd.options.bigquery.ordering_mode = "partial"
        bpd.options.compute.maximum_result_rows = 10_000
        # bpd.options.bigquery.project = "my-project-id"

        try:
            # By loading with the BigQuery engine, you can avoid having to read
            # the file into memory. This is because BigQuery is responsible for
            # parsing the file.
            df = bpd.read_csv(GCS_LOCATION, engine="bigquery")

            # Perform preprocessing. For example, you can map some coded data
            # into a form that is easier to understand.
            df_dates = df.assign(
                ESTIMATE_DATE=df["YEAR"].case_when(
                    caselist=[
                        (df["YEAR"].eq(1), datetime.date(2020, 4, 1)),
                        (df["YEAR"].eq(2), datetime.date(2020, 7, 1)),
                        (df["YEAR"].eq(3), datetime.date(2021, 7, 1)),
                        (df["YEAR"].eq(4), datetime.date(2022, 7, 1)),
                        (df["YEAR"].eq(5), datetime.date(2023, 7, 1)),
                        (df["YEAR"].eq(6), datetime.date(2024, 7, 1)),
                        (True, None),
                    ]
                ),
            ).drop(columns=["YEAR"])
            
            task_instance.xcom_push(key="census_preprocessed_table", value=df_dates.to_gbq())
        finally:
            bpd.close_session()

    def validate_and_write(task_instance):
        import bigframes.pandas as bpd
        bigquery_destination = "swast-scratch.airflow_demo.us_census_by_county2020_to_present"

        bpd.options.bigquery.ordering_mode = "partial"
        bpd.options.compute.maximum_result_rows = 10_000
        # bpd.options.bigquery.project = "my-project-id"

        try:
            # Get the table ID from the previous step.
            bigquery_source = task_instance.xcom_pull(
                task_ids="bf_preprocess",
                key="census_preprocessed_table",
            )
            df_dates = bpd.read_gbq(bigquery_source)

            row_count, column_count = df_dates.shape
            assert row_count > 0
            assert column_count > 0
            assert not df_dates["ESTIMATE_DATE"].hasnans

            # TODO(developer): Add additional validations as needed.

            df_dates.to_gbq(
                bigquery_destination,
                if_exists="replace",
                clustering_columns=["ESTIMATE_DATE", "STATE", "COUNTY"],
            )
        finally:
            bpd.close_session()


    bf_preprocess = PythonOperator(
        task_id="bf_preprocess",
        python_callable=preprocess,
    )

    bf_validate_and_write = PythonOperator(
        task_id="bf_validate_and_write",
        python_callable=validate_and_write,
    )

    cleanup_preprocess_table = BigQueryDeleteTableOperator(
        task_id="cleanup_preprocess_table",
        deletion_dataset_table="{{ task_instance.xcom_pull(task_ids='bf_preprocess', key='census_preprocessed_table') }}",
        # Always execute, even if the previous task failed.
        # https://stackoverflow.com/a/44441890/101923
        trigger_rule="all_done",
    )

    download_upload >> bf_preprocess >> bf_validate_and_write >> cleanup_preprocess_table
    bf_preprocess >> cleanup_preprocess_table
