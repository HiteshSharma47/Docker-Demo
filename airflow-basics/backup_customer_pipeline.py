from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os


DATA_FILE = "/opt/airflow/data/customers.csv"
OUTPUT_DIR = "/opt/airflow/output"

def extract_customers(ti):
    df = pd.read_csv(DATA_FILE)

    print("Customer Extracted")
    print(df)

    ti.xcom_push(
        key="customers",
        value=df.to_dict("records")
    )



def validate_customers(ti):
    customers = ti.xcom_pull(
        task_ids="extract_customers",
        key="customers"
    )

    valid =[]

    for customer in customers:
        if customer["name"] and customer["email"]:
            valid.append(customer)
            print("Valid Customer", customer)
    
    ti.xcom_push(
    key="customers",
    value=valid
    )
    


def load_customers(ti):
    customers = ti.xcom_pull(
        task_ids="validate_customers",
        key="customers"
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(f"{OUTPUT_DIR}/load_customer.txt", "w") as f:

        for customer in customers:
            f.write(f"Loaded: {customer['name']}\n")
        
    print("Database load Successfully")

def send_welcome_email(ti):

    customers = ti.xcom_pull(
        task_ids="extract_customers",
        key="customers"
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(f"{OUTPUT_DIR}/emails_sent.txt", "w") as f:
        
        for customer in customers:
            f.write(f"Sent Email To: {customer['name']} and {customer['email']}\n")
        
        print("Email Sent Successfully!")


with DAG(
    dag_id="customer_onboarding",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers
    )

    validate_task = PythonOperator(
        task_id='validate_customers',
        python_callable=validate_customers
    )

    load_task = PythonOperator(
        task_id='load_customers',
        python_callable=load_customers
    )

    sent_task = PythonOperator(
        task_id='sent_email',
        python_callable=send_welcome_email
    )

    extract_task >> validate_task >> load_task

    extract_task >> sent_task





    
