#!/bin/bash

prefect orion start &>/dev/null & 
prefect deployment build hvd_rating/rate_high_value_dataset.py:get_rating_and_update_dataset -n rate_hvd -t dev &>/dev/null &
prefect deployment apply get_rating_and_update_dataset-deployment.yaml &>/dev/null &
rabbitmq-server start &>/dev/null & 
prefect agent start -t dev &>/dev/null &
python manage.py runscript worker_demon.py -v3 &>/dev/null & 
python3 manage.py process_tasks --queue api_res_operation &>/dev/null & 
python3 manage.py process_tasks --queue create_pipeline &>/dev/null & 
python manage.py runserver 0.0.0.0:8000
