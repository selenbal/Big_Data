import csv
from kafka import KafkaConsumer
import json

#Kafka bağlantısı
consumer = KafkaConsumer('transactions', bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

#CSV dosyasını aç
with open('collected_event.csv', mode='a', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=["account_id", "date", "amount", "transaction_code", "symbol", "price", "total"])
    #Dosyaya header'ı yaz
    writer.writeheader()
    #Kafka'dan mesajları alıp CSV dosyasına yaz
    for message in consumer:
        event = message.value
        writer.writerow(event)
        print("CSV dosyasina yazildi: ", event)
