# Streaming Core Motion Data from Iphone to FastAPI Server

## ios/coremotion_streaming
* the source code for the IOS app. To build the code, ensure the build phase tab under coremotion_streaming targets don't include any files from server
* build the code, then launch a simulator or download it to your iphone to start the app
* to stream to the FastAPI server, you need to get your machine's ip address and change the url string in URLRequest to that IP

## server
* source code for the fastapi server. Open it in vscode, cd into coremotion_streaming/server, and do pip install -r requirements.txt
* need to update ip address in that source code with your own IP adddress for websockets to work correctly 

## Streaming to Kafka (Docker)
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

## Usage:
* start the fastapi web server first ```uvicorn socket_test:app --host <your ip address> --port 8000```
* then open the IOS app on your iphone or simulator. Logs from FastAPI should show 'connection accepted'
* press the 'Start Streaming' button on the IOS app to start streaming to FastAPI server. A simple HTML page shows data being streamed.


