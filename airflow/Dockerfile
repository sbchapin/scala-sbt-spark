FROM apache/airflow:1.10.12
USER root
RUN apt-get update
RUN apt-get install -y gcc
USER airflow
RUN pip install --user 'apache-airflow[slack]==1.10.12'
RUN pip install --user 'apache-airflow[aws]==1.10.12'
RUN pip install --user 'apache-airflow-backport-providers-amazon==2020.6.24'