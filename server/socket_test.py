from fastapi import FastAPI, WebSocket, Request, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

#fastapi templates: https://fastapi.tiangolo.com/advanced/templates/#using-jinja2templates
#fastapi + websockets quickstart: https://fastapi.tiangolo.com/advanced/websockets/#handling-disconnections-and-multiple-clients
app = FastAPI()

templates = Jinja2Templates(directory="./templates")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.websocket("/ws")
async def ws_listen(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            message = await websocket.receive()
            print("Raw message:", message)
            # await websocket.send_json({"echo": data})
    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        print(f"Exception: {e}")
        
