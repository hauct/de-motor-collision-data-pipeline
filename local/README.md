# Step-by-Step Guide for Data Engineering Zoomcamp Project (local version)

This manual provides the steps required to replicate the outcomes of the project:

[Step 0](https://github.com/KazarkinBarys/Data_Engineering_Zoomcamp_Project/blob/main/Local_version/README.md#step-0---preparation) - Preparation

## Project architecture

1. Establish a PostgreSQL database and its environment using `Docker`.
2. Retrieve datasets directly from the source using `Prefect` and `Python`.
3. Process and upload data to the database with the help of `Prefect` and `Python`.
4. Refine, model, and standardize data inside the database through `DBT`.
5. Display the processed and standardized data using `Metabase`.

## Step 0 - preparation
Need to be installed:
  * Windows or Linux
  * Docker and Docker-compose
  * python 3*
  * dbt-postgres
  * pip libraries from requirements.txt
  
In our project working dir, run this script

```
pip install -r requirements.txt
pip install dbt-postgres
```

## Establish a PostgreSQL database and its environment using `Docker`.

Begin by specifying your mount volumes within the docker-compose.yaml file.

Execute the docker-compose command from the "docker" directory:

```bash
docker-compose up -d
```

Upon running this, it will instantiate the PostgreSQL database, along with pgAdmin and Metabase containers, all using the credentials provided in the docker-compose.yaml file.

To establish a connection to the PostgreSQL database through pgAdmin, navigate to http://localhost:8080 in your web browser. Enter the login credentials as specified in the docker-compose.yaml file and proceed to set up the server connection:

|                                |                                |
|--------------------------------|--------------------------------|
|![Alt text](image/pgadmin-1.png)|![Alt text](image/pgadmin-2.png)|
|--------------------------------|--------------------------------|
|![Alt text](image/pgadmin-3.png)|![Alt text](image/pgadmin-4.png)|


## Step 2 and 3 - Retrieve datasets directly from the source, process and upload data to the database using `Prefect` and `Python`

Start the prefect orion service from the "prefect" directory:

```bash
prefect server start
```

Navigate to http://127.0.0.1:4200/blocks and configure the SQLAlchemy Connector block as follows (review the infomation in docker-compose file):

![Alt text](image/prefect-block-alchemy.png)

```
"name": "postgres-connector", "driver": "postgresql+psycopg2", "database": "MVC_db",

"username": "root", "password": "root", "host": "localhost", "port": "5432"
```

Generate the prefect deployment configuration:

```
prefect deployment build ./pipeline.py:MVC_main -n MVC_flow
```

Inspect the MVC_main-deployment.yaml and verify that the "working_dir:" field for file downloads isn't blank. It should match the "path:" entry within the MVC_main-deployment.yaml file.

To execute the new deployment, use:
```
prefect deployment apply MVC_main-deployment.yaml
```

Go to http://127.0.0.1:4200/deployments and edit parameters for downloading and processing data:

![Alt text](image/prefect-deployment-edit.png)

















































































