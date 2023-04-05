FROM python:3 
#ubuntu:18.04
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1


RUN echo 'deb http://deb.debian.org/debian stretch main' >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get autoremove -y && \
    apt-get install -y libssl1.0-dev curl git nano wget screen vim && \
    rm -rf /var/lib/apt/lists/* && rm -rf /var/lib/apt/lists/partial/*

ADD https://github.com/wkhtmltopdf/wkhtmltopdf/releases/download/0.12.4/wkhtmltox-0.12.4_linux-generic-amd64.tar.xz /
RUN set -ex; wget https://github.com/wkhtmltopdf/wkhtmltopdf/releases/download/0.12.4/wkhtmltox-0.12.4_linux-generic-amd64.tar.xz
RUN set -ex; tar xvf /wkhtmltox-0.12.4_linux-generic-amd64.tar.xz
RUN set -ex; mv wkhtmltox/bin/wkhtmlto* /usr/bin/
RUN set -ex; ln -nfs /usr/bin/wkhtmltopdf /usr/local/bin/wkhtmltopdf


COPY . /code/
WORKDIR /code


RUN chmod +x start.sh
RUN chmod +x rabbit.sh
RUN /code/rabbit.sh


RUN pip install -r requirements.txt
RUN pip install psycopg2-binary
RUN python manage.py migrate
RUN python manage.py createcachetable
EXPOSE 8000 15671 15672 25671 25672 4200

CMD ["/code/start.sh"]

