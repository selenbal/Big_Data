from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
import requests
import json
import time

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from datetime import datetime, timedelta
import psycopg2

# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="homework",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    schedule_interval="*/5 * * * *") as dag:

    client = MongoClient("mongodb+srv://cetingokhan:cetingokhan@cluster0.e1cjhff.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

    

    def generate_random_heat_and_humidity_data(dummy_record_count:int):
        import random
        import datetime
        from models.heat_and_humidity import HeatAndHumidityMeasureEvent
        records = []
        for i in range(dummy_record_count):
            temperature = random.randint(10, 40)
            humidity = random.randint(10, 100)
            timestamp = datetime.datetime.now()
            creator = "airflow"
            record = HeatAndHumidityMeasureEvent(temperature, humidity, timestamp, creator)
            records.append(record)
        return records
    
    def save_data_to_mongodb(records):
        db = client["bigdata_training"]
        collection = db["sample_coll"]
        for record in records:
            collection.insert_one(record.__dict__)
        return collection


    def create_sample_data_on_mongodb():
        ### mongodb ye kayıt yapacak method içeriğini tamamlayınız

        records = generate_random_heat_and_humidity_data(10)
        #### eksik parçayı tamamlayınız
        save_data_to_mongodb(records)



    def copy_anomalies_into_new_collection():        
        # sample_coll collectionundan temperature 30 dan büyük olanları new(kendi adınıza bir collectionname) 
        # collectionuna kopyalayın(kendi creatorunuzu ekleyin)
        db = client["bigdata_training"]
        sample_coll = db["sample_coll"]
        new_coll = db["selen_anomalies"]

        anomalies = sample_coll.find({"temperature": {"$gt": 30}})
        try:
            for anomaly in anomalies:
                new_coll.insert_one(anomaly)
        except Exception as e:
            print("Belge eklenirken bir hata oluştu.")
        


    def copy_airflow_logs_into_new_collection():
        
        # airflow veritababnındaki log tablosunda bulunan verilerin son 1 dakikasında oluşan event bazındaki kayıt sayısını 
        # mongo veritabanında oluşturacağınız"log_adınız" collectionına event adı ve kayıt sayısı bilgisi ile 
        # birlikte(güncel tarih alanına ekleyerek) yeni bir tabloya kaydedin.
        # Örn çıktı;
        #{
        #    "event_name": "task_started",
        #    "record_count": 10,
        #    "created_at": "2022-01-01 00:00:00"
        #}
                    
        conn = psycopg2.connect(
            host="postgres",
            port="5432",
            database="airflow",
            user="airflow",
            password="airflow"
        )
    
        cursor = conn.cursor()
        cursor.execute("""
            SELECT event, COUNT(*) as record_count
            FROM log
            WHERE dttm >= NOW() - INTERVAL '1 minute'
            GROUP BY event;
        """)
        
        results = cursor.fetchall()
        
        db = client["bigdata_training"]
        new_log_coll = db["log_selen"]
        
        for event_name, record_count in results:
            document = {
                "event_name": event_name,
                "record_count ": record_count,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            new_log_coll.insert_one(document)
            
        
        cursor.close()
        conn.close()


        

    dag_start = DummyOperator(task_id="start")
    create_sample_data = PythonOperator(task_id="create_sample_data1", python_callable=create_sample_data_on_mongodb, dag=dag)
    copy_anomalies = PythonOperator(task_id="copy_anomalies", python_callable=copy_anomalies_into_new_collection, dag=dag)
    copy_airflow_logs = PythonOperator(task_id="insert_airflow_logs", python_callable=copy_airflow_logs_into_new_collection, dag=dag)
    final_task = DummyOperator(task_id="final_task")


    #Dependencies
    dag_start >> create_sample_data
    dag_start >> copy_airflow_logs >> final_task
    create_sample_data >> copy_anomalies >> final_task



