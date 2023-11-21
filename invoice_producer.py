from confluent_kafka import Producer
import json
import time

class InvoiceProducer:
    def __init__(self) -> None:
        self.topic = "invoices"
        self.conf = {'bootstrap.servers': 'pkc-0ww79.australia-southeast2.gcp.confluent.cloud:9092',
                     'security.protocol': 'SASL_SSL',
                     'sasl.mechanism': 'PLAIN',
                     'sasl.username': 'XYTZZD6HNTJTBUWP',
                     'sasl.password': 'r+iUkNSrxGe0AtTVmYit217deTCl8pMxDnOgX40k5NGMjz9Uw8XZOnIKOYtOICRC',
                     'client.id': "allenchen-laptop"}
        
    def deliver_callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            key = msg.key().decode('utf-8')
            invoice_id = json.loads(msg.value().decode('utf-8'))["InvoiceNumber"]
            print(f"Produced event to : key = {key} value = {invoice_id}")
    
    def produce_invoices(self, producer, counts):
        counter = 0
        with open('data/invoices.json') as lines:
            for line in lines:
                invoice = json.loads(line)
                store_id = invoice['StoreID']
                producer.produce(topic=self.topic, key=store_id, value=line, callback=self.deliver_callback)
                time.sleep(0.5)
                producer.poll(1)
                if counter == counts:
                    break
                counter = counter + 1
    
    def start(self):
        kafka_producer = Producer(self.conf)
        self.produce_invoices(kafka_producer, 10)
        # wait 10 more secs for the acknowledge at the stop time if it is not received
        kafka_producer.flush(10)

if __name__ == "__main__":
    invoice_producer = InvoiceProducer()
    invoice_producer.start()








