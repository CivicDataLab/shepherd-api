#!/bin/bash

prefect orion start &>/dev/null & 
rabbitmq-server start &>/dev/null & 
python manage.py runscript worker_demon.py -v3 &>/dev/null & 
python3 manage.py process_tasks --queue api_res_operation &>/dev/null & 
python3 manage.py process_tasks --queue create_pipeline &>/dev/null & 
python manage.py runserver 0.0.0.0:8000
