# Streaming Core Motion Data from Iphone to FastAPI Server

## ios/coremotion_streaming
* the source code for the IOS app. To build the code, ensure the build phase tab under coremotion_streaming targets don't include any files from server
* build the code, then launch a simulator or download it to your iphone to start the app
* to stream to the FastAPI server, you need to get your machine's ip address and change the url string in URLRequest to that IP

## server
* source code for the fastapi server. Open it in vscode, cd into coremotion_streaming/server, and do pip install -r requirements.txt
* need to update ip address in that source code with your own IP adddress for websockets to work correctly 

## Usage:
* start the fastapi web server first ```uvicorn socket_test:app --host <your ip address> --port 8000```
* then open the IOS app on your iphone or simulator. Logs from FastAPI should show 'connection accepted'
* press the 'Start Streaming' button on the IOS app to start streaming to FastAPI server. A simple HTML page shows data being streamed.


