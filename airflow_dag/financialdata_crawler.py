from datetime import datetime

from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'chengche',
    'depends_on_past': False,
    'start_date': days_ago(1)
}

dag = DAG(
    'financialdata_crawler',
    default_args=default_args,
    description='crawler taifex data',
    schedule_interval='0 11 * * *',
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

today = datetime.now().strftime('%Y-%m-%d')

run_task_1 = BashOperator(
    task_id='largeTraderTutQry_TX',
    bash_command=f"sudo -i -u ppp1987524 -H sh -c 'cd /home/ppp1987524/TFE-data; poetry run python financialdata/crawler/largeTraderFutQry_TX.py {today} {today}; '",
    dag=dag
)

run_task_2 = BashOperator(
    task_id='futDailyMarketRoprot_MTX',
    bash_command=f'sudo -i -u ppp1987524 -H sh -c \'cd /home/ppp1987524/TFE-data; poetry run python financialdata/crawler/futDailyMarketRoprot_MTX.py {today} {today}; \'',
    dag=dag
)

run_task_3 = BashOperator(
    task_id='futContractsCate_TXF',
    bash_command=f'sudo -i -u ppp1987524 -H sh -c \'cd /home/ppp1987524/TFE-data; poetry run python financialdata/crawler/futContractsDate_TXF.py {today} {today}; \'',
    dag=dag
)

run_task_4 = BashOperator(
    task_id='futContractsCate_MXF',
    bash_command=f'sudo -i -u ppp1987524 -H sh -c \'cd /home/ppp1987524/TFE-data; poetry run python financialdata/crawler/futContractsDate_MXF.py {today} {today}; \'',
    dag=dag
)

start >> run_task_1 >> run_task_2 >> run_task_3 >> run_task_4 >> end
