from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable

from datetime import datetime
import requests
import pandas as pd

END_POINT = 'https://api.nytimes.com/svc/news/v3/content/nyt/business.json?api-key='
API_KEY = Variable.get('API_KEY')

args = {
    'owner': 'admin',
    'start_date': datetime(2018, 11, 1),
    'provide_context': True
}


def extract_data(**kwargs):
    ti = kwargs['ti']
    response = requests.get(END_POINT + API_KEY)

    if response.status_code == 200:
        json_data = response.json()
        title = []
        abstract = []
        url = []
        created_date = []

        for i in range(len(json_data['results'])):
            if (json_data['results'][i]['item_type']) == 'Article' and (json_data['results'][i]['abstract'] != ''):
                title.append(json_data['results'][i]['title'])
                abstract.append(json_data['results'][i]['abstract'])
                url.append(json_data['results'][i]['url'])
                created_date.append(json_data['results'][i]['created_date'])

        data = {'title': title,
                'abstract': abstract,
                'url': url,
                'created_date': created_date}

        df = pd.DataFrame(data=data)

        ti.xcom_push(key='nyt_raw_data', value=df)
        Variable.set("DF_LEN", len(ti.xcom_pull(key='nyt_raw_data', task_ids=['extract_data'])[0]))


with DAG('NYT_API_DAG',
         description='Every day get business news from New York Times API',
         schedule_interval='*/1 * * * *', # 50 6 * * * every day at 6:50
         catchup=False,
         default_args=args) as dag:
    extract_data = PythonOperator(task_id='extract_data',
                                  python_callable=extract_data)

    create_raw_data_table = PostgresOperator(task_id="create_raw_data_table",
                                             postgres_conn_id="NYT_business_news_database",
                                             sql="""CREATE TABLE IF NOT EXISTS raw_data (
                                                       date VARCHAR(50) PRIMARY KEY,
                                                       title VARCHAR(250) NOT NULL,
                                                       abstract VARCHAR(250) NOT NULL,
                                                       url VARCHAR(250) UNIQUE NOT NULL);""")

    insert_in_raw_data_table = PostgresOperator(task_id="insert_raw_table",
                                                postgres_conn_id="NYT_business_news_database",
                                                sql=[f"""INSERT INTO raw_data VALUES(
                                                            '{{{{ti.xcom_pull(key='nyt_raw_data',
                                                            task_ids=['extract_data'])[0].iloc[{i}]['created_date']}}}}',
                                                            '{{{{ti.xcom_pull(key='nyt_raw_data',
                                                            task_ids=['extract_data'])[0].iloc[{i}]['title']}}}}',
                                                            '{{{{ti.xcom_pull(key='nyt_raw_data',
                                                            task_ids=['extract_data'])[0].iloc[{i}]['abstract']}}}}',
                                                            '{{{{ti.xcom_pull(key='nyt_raw_data',
                                                            task_ids=['extract_data'])[0].iloc[{i}]['url']}}}}')"""
                                                     for i in range(int(Variable.get('DF_LEN')))])

    extract_data >> create_raw_data_table >> insert_in_raw_data_table
