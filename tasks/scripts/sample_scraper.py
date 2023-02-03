import json
import pandas as pd
import pika
import requests
from bs4 import BeautifulSoup
from s3_utils import *

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
result = channel.queue_declare('', exclusive=False, durable=True)
queue_name = result.method.queue

print("queue name----", queue_name)
binding_key = "example_scraper"

channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key=binding_key)



def sample_scraper(url):
    # Set the URL of the website you want to scrape

    # Send an HTTP GET request to the website and retrieve the HTML page
    response = requests.get(url)

    # Parse the HTML page
    soup = BeautifulSoup(response.text, "html.parser")

    # Find all the elements with the specified class
    elements = soup.find("h1")
    return elements

def on_request(ch, method, props, body):
    print("[x] received task message...")
    task_details = json.loads(body)
    context = task_details["context"]
    try:
        response = sample_scraper(context["url"])
        if isinstance(response, pd.core.frame.DataFrame):
            response_msg = response.to_csv()
        else:
            response_msg = response
            print(response_msg)
            with open("sample_scraper.txt", "wb") as f:
                f.write(str(response_msg.text))
            s3_link = upload_result("sample_scraper.txt")
        ch.basic_publish(exchange="",
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=props.correlation_id,delivery_mode=2),
                         body=str(s3_link))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("[x] sent the response to the client..")
    except Exception as e:
        raise e
channel.basic_qos(prefetch_count=2)
channel.basic_consume(queue=queue_name, on_message_callback=on_request)

print(" [x] Awaiting RPC requests")

channel.start_consuming()