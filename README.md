# MLE-Airflow
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Ubuntu](https://img.shields.io/badge/Ubuntu-E95420?style=for-the-badge&logo=ubuntu&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)
![Telegram](https://img.shields.io/badge/Telegram-2CA5E0?style=for-the-badge&logo=telegram&logoColor=white)

Repository for the MLE-Airflow project with a simple example of how to leverage Apache Airflow for managing machine learning workflows.

## Description

MLE-Airflow is a project designed to give a simple example of how to use Apache Airflow for managing ML workflows based on the telecompany churn dataset stored in a PostgreSQL database.

In the given project, the following aspects of Airflow are covered:
- DAGs (two different implementations: via decorators and via operators)
- Plugins (for DAGs and notifications to Telegram)
- Hooks (for interacting with the source and destination databases)

## Repository Structure

**[docker-compose.yaml](docker-compose.yaml)**: This file contains the configuration for the Docker Compose for running the Airflow service.

**[Dockerfile](Dockerfile)**: This Dockerfile sets up a Python with all necessary packages for all subservices.

**[requirements.txt](requirements.txt)**: This file contains the list of Python packages required for the project.

**[dags](dags)**: This directory contains Python code for Airflow DAGs. It includes DAGs for two stages:
- ETL for Data Preparation: [churn.py](dags/churn.py), [alt_churn.py](dags/churn.py)
- ETL for Data Cleaning: [clean_churn.py](dags/clean_churn.py)

**[plugins](plugins)**: This directory contains Python code for Airflow plugins. It includes plugins for:
- Data Preparation DAG: [churn.py](plugins/steps/churn.py)
- Data Cleaning DAG: [clean_churn.py](plugins/steps/clean_churn.py)
- Telegram callback (to notify about the status of the DAGs execution): [messages.py](plugins/steps/messages.py)

**[notebooks](notebooks)**: This directory contains Jupyter notebooks for data exploration and some testing.


## Getting Started

In order to start the Airflow, use the following steps:

1. Clone the current repository and cd to the root of the project

2. Create directories for logs, plugins, and configuration files used by Airflow:
    ```
    mkdir -p ./dags ./logs ./plugins ./config 
    ```

3. Save AIRFLOW_UID of your machine to the .env file:
    ```
    echo -e "\nAIRFLOW_UID=$(id -u)" >> .env 
    ```

4. Create an account with an Airflow login and password for the web UI:
    ```
    docker compose up airflow-init 
    ```

5. Clear possible cache that resulted from the last step:
    ```
    docker compose down --volumes --remove-orphans
    ```

6. Start the Airflow. Use this command every time you want to start the Airflow:
    ```
    docker compose up --build 
    ```

You can access the Airflow web UI at http://your_host_ip_address:8080/, where your_host_ip_address is the IP address of the machine where the Airflow is running.

You can configure hooks and run DAGs in the Airflow web UI. To learn about how to do it and the Airflow, visit the [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html).

PS: Unfortunately, the telegram callback plugin only works via request library, but not via TelegramHook.





