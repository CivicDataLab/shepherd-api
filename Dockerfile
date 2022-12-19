FROM python:3 
#ubuntu:18.04
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY . /code/
WORKDIR /code

RUN apt update
RUN apt install -y screen curl vim 

RUN chmod +x start.sh
RUN chmod +x rabbit.sh
RUN /code/rabbit.sh



RUN pip install -r requirements.txt
RUN pip install psycopg2-binary
RUN python manage.py migrate
EXPOSE 8000 15671 15672 25671 25672 4200

CMD ["/code/start.sh"]

