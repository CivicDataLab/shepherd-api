import json
import os

import pika
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select

current_folder = os.path.realpath(os.path.join(__file__, '..'))

options = Options()
options.headless = True
options.add_argument("disable-infobars")
options.add_argument("--disable-gpu")
# options.set_preference('browser.download.dir', os.path.join(current_folder, 'downloads'))
path_to_chrome_driver = "E:/chromedriver"

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
result = channel.queue_declare('', exclusive=False, durable=True)
queue_name = result.method.queue

print("queue name----", queue_name)
binding_key = "obi_mh_scraper"

channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key=binding_key)


def get_mh_district_wise_budget_data(fiscal_years: list):
    driver = webdriver.Chrome(options=options, executable_path=path_to_chrome_driver)
    driver.get(
        "https://beams.mahakosh.gov.in/Beams5/BudgetMVC/MISRPT/DistrictWiseExpenditure.jsp"
    )

    for fiscal_year in fiscal_years:
        Select(driver.find_element(By.ID, 'year')).select_by_visible_text(fiscal_year)

        try:
            msg = 'SUCCESS - File downloaded for {}\n'.format(fiscal_year)

            driver.find_element(
                By.XPATH, "//input[@class='submit'][1]"
            ).click()
            return "Done"

        except:
            msg = 'FAIL - File not downloaded for {}\n'.format(fiscal_year)
            return "Worker failed with an error"
        finally:
            with open(os.path.join(current_folder, 'downloads.log'), 'a+') as fp:
                fp.write(msg)

            print(msg)

    driver.close()


def on_request(ch, method, props, body):
    print("[x] received task message...")
    task_details = json.loads(body)
    context = task_details["context"]
    fiscal_years = context['fiscal_years']
    try:
        response = get_mh_district_wise_budget_data(fiscal_years)
        ch.basic_publish(exchange="",
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=props.correlation_id,delivery_mode=2),
                         body=str(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("[x] sent the response to the client..")
    except Exception as e:
        raise e


channel.basic_qos(prefetch_count=2)
channel.basic_consume(queue=queue_name, on_message_callback=on_request)

print(" [x] Awaiting RPC requests")

channel.start_consuming()