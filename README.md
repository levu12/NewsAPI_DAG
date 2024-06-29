#  NewsAPI_DAG

A simple DAG for NewsAPI using Apache Airflow: https://airflow.apache.org/

## Getting Started

On Linux:

Download both dag.py and newsetl.py from the repository

Install SQLite

Create database, change DATABASE = '/mnt/d/newsdb.db' in newsetl.py to the path to your database

Get API key from https://newsapi.org/

Run
```
export AIRFLOW_VAR_NEWSAPI="YOUR_API_KEY"
source ~/.bashrc
```

Install Apache Airflow

Set load_examples = False in airflow.cfg in the ~/airflow folder

Make sure that airflow_home and dags_folder in airflow.cfg also point to /home/YOURUSERNAME/airflow/dags. If the folder does not exist, be sure to create it.

Drag both dag.py and newsetl.py into the dags folder and run

```
export AIRFLOW_HOME="/home/YOURUSERNAME/airflow/dags"
airflow list_dags
airflow standalone
```

Your DAG should appear in the dashboard after signing it with the provided credentials.

You might want to create a variable as well containing your API key with the name NEWSAPI.

Now, run the DAG manually. It should run! 

## Help

Please open an issue if you require help.
