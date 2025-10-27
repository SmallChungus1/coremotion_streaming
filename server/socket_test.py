from fastapi import FastAPI, WebSocket, Request, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import json

#fastapi templates: https://fastapi.tiangolo.com/advanced/templates/#using-jinja2templates
#fastapi + websockets quickstart: https://fastapi.tiangolo.com/advanced/websockets/#handling-disconnections-and-multiple-clients
app = FastAPI()
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

            for a_websock in connections:
                try:
                    await a_websock.send_json(message_json_str)
                except Exception as e:
                    print(f"Exception at websocket send in ws_listen: {e}")
    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        print(f"Exception in ws_listen: {e}")
        
