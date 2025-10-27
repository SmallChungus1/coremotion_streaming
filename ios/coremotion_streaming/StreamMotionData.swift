//
//  StreamMotionData.swift
//  coremotion_streaming
//
//  Created by Hanson Li on 10/27/25.
//


//startscream quick start: https://github.com/daltoniam/Starscream?tab=readme-ov-file | https://medium.com/@sreejithbhatt/real-time-networking-in-ios-websockettask-vs-socket-io-vs-starscream-vs-server-sent-events-1111b1992de1

import Foundation
import Starscream

class StreamMotionData : WebSocketDelegate {
    
    var socket: WebSocket!
    init() {
        var request = URLRequest(url: URL(string: "ws://192.168.1.227:8000/ws")!) //ws or wss for websockets with startscream.
        request.timeoutInterval = 5
        socket = WebSocket(request: request)
        socket.delegate = self
        socket.connect()
    }
    
    func didReceive(event: Starscream.WebSocketEvent, client: any Starscream.WebSocketClient) {
        switch event {
        case .connected(_):
            print("Connected")
        case .disconnected(let reason, _):
            print("Disconnected: \(reason)")
        case .text(let string):
            print("Received text: \(string)")
        case .binary(let data):
            print("Received data: \(data)")
        default:
            break
        }
    }
    
    func writeEntireData(dict: [String:[Double]]){
        guard let json = try? JSONSerialization.data(withJSONObject: dict) else {
            print("JSON encoding failed")
            return
        } //convert to json bytes
        self.socket.write(data: json, completion: .none)
    }
    
    func writeSingleData(pitch: Double, roll: Double, yaw: Double){
        let cur_data_packet = ["pitch": pitch, "roll": roll, "yaw": yaw]
        
        guard let json = try? JSONSerialization.data(withJSONObject: cur_data_packet) else {
            print("JSON encoding failed")
            return
        }
        
        self.socket.write(data: json, completion: .none)
    }
    
    func websocketDisconnect(){
        self.socket.disconnect()
    }
}
