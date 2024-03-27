from pymongo import MongoClient
from kafka import KafkaProducer
import json
import time
import random

#MongoDB bağlantısı

client = MongoClient("mongodb+srv://cetingokhan:cetingokhan@cluster0.e1cjhff.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["sample_analytics"]
collection = db["transactions"]

# Kafka bağlantısı
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    random_doc = collection.find_one()
    if random_doc:
        account_id = random_doc['account_id']
        transactions = random_doc['transactions']
        random_transactions = random.sample(transactions, random.randint(5, 10))
        for transaction in random_transactions:
            message = {
                "account_id": account_id,
                "date": str(transaction['date']),
                "amount": transaction['amount'],
                "transaction_code": transaction['transaction_code'],
                "symbol": transaction['symbol'],
                "price": transaction['price'],
                "total": transaction['total']
            }
            producer.send('transactions', value=message)
            print("Gönderildi: ", message)
    time.sleep(2)

