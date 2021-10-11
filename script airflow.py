# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'Fadi Alsalti',
    'start_date': days_ago(0),
    'email': ['fadi.alsalti@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the first task named unzip_data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz; gunzip -c tolldata.tgz | tar xvf -',
    dag=dag,
)

# define the second task named extract_data_from_csv
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)

# define the third task named extract_data_from_tsv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 tollplaza-data.tsv > tsv_data.csv',
    dag=dag,
)

# define the fourth task named extract_data_from_fixed_width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='awk "{print $10,$11}" payment-data.txt > fixed_width_data.csv',
    dag=dag,
)

# define the fifth task named consolidate_data
# replace space with comma in the tsv and txt data files and paste
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='cat tsv_data.csv | tr [:blank:] "," > tsv_data_tr.csv; cat fixed_width_data.csv | tr [:blank:] "," > fixed_width_data_tr.csv; paste -d"," csv_data.csv tsv_data_tr.csv fixed_width_data_tr.csv > extracted_data.csv;',
    dag=dag,
)

# define the sixth task named transform_data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr "[a-z]" "[A-Z]" < extracted_data.csv > transformed_data.csv',
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data