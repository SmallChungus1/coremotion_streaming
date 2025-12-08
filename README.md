# Streaming Core Motion Data from Iphone to FastAPI Server

## ios/coremotion_streaming
* the source code for the IOS app. To build the code, ensure the build phase tab under coremotion_streaming targets don't include any files from server
* build the code, then launch a simulator or download it to your iphone to start the app
* to stream to the FastAPI server, you need to get your machine's ip address and change the url string in URLRequest to that IP if hosting FastAPI server locally. If hosting FastAPI as Azure appserivce, use their provided url instead.

## FastAPI/Websocket via Azure AppService (New)
* Use the provided dockerfile inside the **server** folder to build a Docker Image
* Push docker image to Azure eventhub

## FastAPI/Websocket Server (Old, locally hosted option)
* source code for the fastapi server. Open it in vscode, cd into coremotion_streaming/server, and do pip install -r requirements.txt
* need to update ip address in that source code with your own IP adddress for websockets to work correctly 
* note: with this setup, you can only stream to FastAPI if your phone is on the samenetwork as the FastAPI server

## Streaming to kafka/Azure Eventhub (New)
* On Azure, setup proivision an Azure Eventhub with the Standard Tier (basic tier doesn't offer Kafka protocol)
* Configure .env file to connect to Azure eventhub from Spark pipeline defined in the spark_pipeline folder, include the following in the env file:
    * event-hub-namespace
    * event-hub-topic
    * event-hub-primary-conn-str
    * event-hub-bootstrap-server

## Streaming to Kafka (Locally Hosted Docker, Old approach)
* follow kafka setup guide here to setup kafka locally and use the FastAPI server to write streamd data from our IOS App to Kafka topic: https://kafka.apache.org/quickstart
    * note: they recommend using docker setup no need to download the kafka zip file
* Install Docker Desktop
* run the following commands to spin up and enter docker kafka container
    * ```docker pull apache/kafka:4.1.0```
    * ```docker run -p 9092:9092 apache/kafka:4.1.0``` if using another port number, update the kafka_host_addr in socket_test.py
    * ```docker exec -it <image id> bash``` to open a shell for the container
* ```/opt/kafka/bin/kafka-topics.sh --create --topic <topic name> --bootstrap-server localhost:9092``` to create a Kafka topic for storing streamed data
    * note: may need to use ```find / -name "kafka-topics.sh"``` to get location of the kafka-topics.sh file
* can start streaming data by launching the fastAPI server. You can see the streamed data stored in the kafka topic using ```/opt/kafka/bin/kafka-console-consumer.sh --topic <topic name> --from-beginning --bootstrap-server localhost:9092```

## Kafka to Snowflake via Spark:
* we use Spark batch jobs orchestrated via airflow to process and push data into snowflake
* ```python spark_pipeline/kafka_to_snowflake.py``` to run locally
* Use our **spark_email_dag** with airflow to trigger daily airflow jobs that bundles data processing, data cleaning, and analytics creationg/emailing.
* In the .env file of spark_pipeline, to connect to snowflake via Spark, include the following:
    * sfURL
    * sfUser
    * sfDatabase
    * sfSchema
    * sfWarehouse
    * rsakey_path
    * tableName


## ML Model training: 
* All code is contained in the ml_models folder, synthetic data generation for driving behavior model handled by synthetic_data_gen.py. Driving behavior model training handeled by train_transformer_driving.py, and our old driving vs walking model code is analysis_algo_old.py
* To train transformer driving behavior model:
    * Need to download snowflake table as csv, drag it into ml_models folder, update source_csv_path path name to the appropriate name
    * run syntehtic_data_gen.py to generate labels for real data + labeled synthetic data
    * export PYTORCH_ENABLE_MPS_FALLBACK=1 (if using mac with MPS)
    * run ```python ml_models/train_transfomer_driving.py``` to train the model. Save a copy of the weights in the analytics folder for model inference for email reports.

## Analytics Emails:
* Airflow orchestrated job to query data for each user (identified by their email), make charts on daily/weekly level data, do anomoly detection via hard-coded rules, model inference for safe vs risky driving behavior.
* currently set up to send emails from personal gmail accounts
* charts.py in analytics folder contains all logic for chart creation + model inference + anomoly detection. Once you train a model, move the model weights '.pth' into the analytics folder. Make a copy of the .env file in spark_pipline folder in the analytics folder and include the following:
    * localTZ="America/Chicago"
    * emailSender
    * gmailAppPass
    * gmailAppName
* this link to learn about Google AppPass: https://support.google.com/accounts/answer/185833?hl=en

## Airflow:
* run command ```pip install "apache-airflow[celery]==3.1.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.3/constraints-3.13.txt"``` to install airflow (or check https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html for install instructions)
    * swap out constraints-3.13.txt with your python version if it's not 3.13
* then, set airflow home: ```export AIRFLOW_HOME=~/airflow```
* (optional, if getting SIGSEGV error when running airlow:) ```export no_proxy='*'```
* then, run ```airflow standalone``` to start airflow. Find this line in the terminal output 'Password for the admin user has been previously generated in' that provides admin login for localhost:8080 portal. (Optional) before running standalaone, ```export PYTHONFAULTHANDLER="true"``` for more detailed airflow error logs
* Important: after airflow is spun up, go to dashboard, click on: Admin -> Connections -> Add Connection -> Connection ID: 'spark_local', Connection Type: 'Spark', Host: 'local' 

## Usage:
* Once FastAPI + Eventhub/Kafka is setup on Azure, you can start streaming data
* then open the IOS app on your iphone or simulator. Logs from FastAPI should show 'connection accepted'
* press the 'Start Streaming' button on the IOS app to start streaming to FastAPI server. A simple HTML page on Azure Appservice shows data being streamed in real time.
* press 'Stop' to stop the stream
* Spin up airflow cluster to run Spark pipline + Email analytics daily


