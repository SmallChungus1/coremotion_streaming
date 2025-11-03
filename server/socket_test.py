from fastapi import FastAPI, WebSocket, Request, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import json
from kafka import KafkaProducer
from contextlib import asynccontextmanager


kafka_host_addr = "localhost:9092"
kafka_topic = "test-topic1"
#Fastapi use kafka-python's Kafka producer to send streamed data from iphone to Kafka Cluster
#kafka-python set up https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html | https://kafka-python.readthedocs.io/en/master/

try: 
    producer = KafkaProducer(bootstrap_servers=kafka_host_addr)
except Exception as e:
    print(f"Error in initalizing KafkaProducer: {e} continuning without it")
    producer = None


#flush producer buffer upon fastapi app shutdown with FasAPI lifespan: https://fastapi.tiangolo.com/advanced/events/
@asynccontextmanager
async def lifespan(app: FastAPI):
    #startup section is before yield
    print("Fastapi app start up and performing tasks pre-startup")
    
    yield
    
    #shutdown section is after yeild
    print("Fastapi app shutting down - performing tasks pre-shutdown")
    if producer:
        producer.flush()
        producer.close()

#fastapi templates: https://fastapi.tiangolo.com/advanced/templates/#using-jinja2templates
#fastapi + websockets quickstart: https://fastapi.tiangolo.com/advanced/websockets/#handling-disconnections-and-multiple-clients
app = FastAPI(lifespan=lifespan)
connections = set()

templates = Jinja2Templates(directory="./templates")



    
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


#app and html page opens their own websockets. To stream ios data onto html page, must send the message to all websockets
@app.websocket("/ws")
async def ws_listen(websocket: WebSocket):
    await websocket.accept()
    connections.add(websocket)
    try:
        while True:
            message = await websocket.receive_bytes()
            print("Raw message:", message)
            message_json_str = message.decode('utf-8')
            
            #need to send bytes
            #TODO: investigate if producer.send is synchrous and blocking
            if producer:
                producer.send(topic=kafka_topic, value=message)

            for a_websock in connections:
                try:
                    await a_websock.send_json(json.loads(message_json_str))
                except Exception as e:
                    print(f"Exception at websocket send in ws_listen: {e}")
    except WebSocketDisconnect:
        print("Client disconnected")
        connections.remove(websocket)

    except Exception as e:
        print(f"Exception in ws_listen: {e}")
        
