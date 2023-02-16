from datetime import datetime

from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

APP_USER = 'ppp1987525'

default_args = {
    'owner': 'chengche',
    'depends_on_past': False,
    'start_date': '2023-02-02'
}

dag = DAG(
    'pg_2_gcs',
    default_args=default_args,
    description='crawler taifex data',
    schedule_interval='0 13 * * *',
    tags=['taifex'],
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

run_task_1 = BashOperator(
    task_id='largeTraderTutQry_TX_dump_2_csv',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'cd ~/TFE-data; poetry run python financialdata/pg_extract/pg_extract.py large_trader_fut_qry; '",
    dag=dag
)

run_task_2 = BashOperator(
    task_id='futDailyMarketRoprot_MTX_dump_2_csv',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'cd ~/TFE-data; poetry run python financialdata/pg_extract/pg_extract.py fut_daily_market_roprot_mtx; '",
    dag=dag
)

run_task_3 = BashOperator(
    task_id='futContractsCate_TXF_dump_2_csv',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'cd ~/TFE-data; poetry run python financialdata/pg_extract/pg_extract.py fut_contracts_date_tfx; '",
    dag=dag
)

run_task_4 = BashOperator(
    task_id='futContractsCate_MXF_dump_2_csv',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'cd ~/TFE-data; poetry run python financialdata/pg_extract/pg_extract.py fut_contracts_date_mfx; '",
    dag=dag
)

run_task_1_1 = BashOperator(
    task_id='largeTraderTutQry_TX_upload_2_gsc',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'gsutil cp ~/TFE-data/financialdata/pg_extract/data/large_trader_fut_qry.csv gs://tfe-data-bucket/'",
    dag=dag
)

run_task_2_1 = BashOperator(
    task_id='futDailyMarketRoprot_MTX_upload_2_gsc',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'gsutil cp ~/TFE-data/financialdata/pg_extract/data/fut_daily_market_roprot_mtx.csv gs://tfe-data-bucket/'",
    dag=dag
)

run_task_3_1 = BashOperator(
    task_id='futContractsCate_TXF_upload_2_gsc',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'gsutil cp ~/TFE-data/financialdata/pg_extract/data/fut_contracts_date_tfx.csv gs://tfe-data-bucket/'",
    dag=dag
)

run_task_4_1 = BashOperator(
    task_id='futContractsCate_MXF_upload_2_gsc',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'gsutil cp ~/TFE-data/financialdata/pg_extract/data/fut_contracts_date_mfx.csv gs://tfe-data-bucket/'",
    dag=dag
)

run_task_1_2 = BashOperator(
    task_id='largeTraderTutQry_TX_bq_update',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'bq --location=asia-east1 load --autodetect --replace --source_format=CSV TFE_data.large_trader_fut_qry gs://tfe-data-bucket/large_trader_fut_qry.csv'",
    dag=dag
)

run_task_2_2 = BashOperator(
    task_id='futDailyMarketRoprot_MTX_bq_update',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'bq --location=asia-east1 load --autodetect --replace --source_format=CSV TFE_data.fut_daily_market_roprot_mtx gs://tfe-data-bucket/fut_daily_market_roprot_mtx.csv'",
    dag=dag
)

run_task_3_2 = BashOperator(
    task_id='futContractsCate_TXF_bq_update',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'bq --location=asia-east1 load --autodetect --replace --source_format=CSV TFE_data.fut_contracts_date_tfx gs://tfe-data-bucket/fut_contracts_date_tfx.csv'",
    dag=dag
)

run_task_4_2 = BashOperator(
    task_id='futContractsCate_MXF_bq_update',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'bq --location=asia-east1 load --autodetect --replace --source_format=CSV TFE_data.fut_contracts_date_mfx gs://tfe-data-bucket/fut_contracts_date_mfx.csv'",
    dag=dag
)

upload_finish = DummyOperator(
    task_id='upload_finish',
    dag=dag
)


run_task_5 = BashOperator(
    task_id='dbt_run_TFE_transform',
    bash_command=f"sudo -i -u {APP_USER} -H sh -c 'cd ~/TFE-data/dbt_task; dbt run -s TFE_transform'",
    dag=dag
)

start >> run_task_1 >> run_task_1_1 >> run_task_1_2 >> upload_finish
start >> run_task_2 >> run_task_2_1 >> run_task_2_2 >> upload_finish
start >> run_task_3 >> run_task_3_1 >> run_task_3_2 >> upload_finish
start >> run_task_4 >> run_task_4_1 >> run_task_4_2 >> upload_finish

upload_finish >> run_task_5 >> end