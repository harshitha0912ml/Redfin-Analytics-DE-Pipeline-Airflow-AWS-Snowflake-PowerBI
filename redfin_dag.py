from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3

# S3 Setup
s3_client = boto3.client('s3')
source_bucket_name = 'redfinrawdata0912'
target_bucket_name = 'redfinanalytics0912'
s3_key = 'redfindata_cleaned.csv'

# -------------------------
# Step 1: Extract from S3
# -------------------------
def extract_from_s3(**kwargs):
    now = datetime.now()
    date_now_string = now.strftime("%d%m%Y%H%M%S")
    file_str = f"redfin_data_{date_now_string}"
    local_path = f"/home/ubuntu/{file_str}.csv"

    s3_client.download_file(source_bucket_name, s3_key, local_path)

    # Push both local path and file ID to XCom
    return [local_path, file_str]

# -------------------------
# Step 2: Transform
# -------------------------
def transform_data(task_instance):
    data = task_instance.xcom_pull(task_ids="tsk_extract_redfin_data")[0]
    object_key = task_instance.xcom_pull(task_ids="tsk_extract_redfin_data")[1]
    df = pd.read_csv(data)

    # Remove commas from the 'city' column
    df['city'] = df['city'].str.replace(',', '')
    cols = [
        'period_begin', 'period_end', 'period_duration', 'region_type', 'region_type_id',
        'table_id', 'is_seasonally_adjusted', 'city', 'state', 'state_code', 'property_type',
        'property_type_id', 'median_sale_price', 'median_list_price', 'median_ppsf',
        'median_list_ppsf', 'homes_sold', 'inventory', 'months_of_supply', 'median_dom',
        'avg_sale_to_list', 'sold_above_list', 'parent_metro_region_metro_code', 'last_updated'
    ]
    df = df[cols].dropna()

    df['period_begin'] = pd.to_datetime(df['period_begin'])
    df['period_end'] = pd.to_datetime(df['period_end'])

    df["period_begin_in_years"] = df['period_begin'].dt.year
    df["period_end_in_years"] = df['period_end'].dt.year
    df["period_begin_in_months"] = df['period_begin'].dt.month
    df["period_end_in_months"] = df['period_end'].dt.month

    month_dict = {
        "period_begin_in_months": {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        },
        "period_end_in_months": {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }
    }
    df = df.replace(month_dict)

    print('Num of rows:', len(df))
    print('Num of cols:', len(df.columns))

    # Upload cleaned data to target S3
    csv_data = df.to_csv(index=False)
    output_key = f"{object_key}.csv"
    s3_client.put_object(Bucket=target_bucket_name, Key=output_key, Body=csv_data)

# -------------------------
# Default DAG Args
# -------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 4),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

# -------------------------
# DAG Definition
# -------------------------
dag = DAG(
    dag_id='redfin_analytics_dag',
    default_args=default_args,
    catchup=False,
)
dag.schedule_interval = '@weekly'

# -------------------------
# DAG Tasks
# -------------------------
with dag:
    extract_redfin_data = PythonOperator(
        task_id='tsk_extract_redfin_data',
        python_callable=extract_from_s3
    )

    transform_redfin_data = PythonOperator(
        task_id='tsk_transform_redfin_data',
        python_callable=transform_data
    )

    load_to_s3 = BashOperator(
        task_id='tsk_load_to_s3',
        bash_command='aws s3 mv {{ ti.xcom_pull("tsk_extract_redfin_data")[0] }} s3://redfinrawdata0912/archived/'
    )

    extract_redfin_data >> transform_redfin_data >> load_to_s3
