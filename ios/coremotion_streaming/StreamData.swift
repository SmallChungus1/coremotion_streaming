//
//  StreamMotionData.swift
//  coremotion_streaming
//
//  Created by Hanson Li on 10/27/25.
//


//startscream quick start: https://github.com/daltoniam/Starscream?tab=readme-ov-file | https://medium.com/@sreejithbhatt/real-time-networking-in-ios-websockettask-vs-socket-io-vs-starscream-vs-server-sent-events-1111b1992de1

import Foundation
import Starscream

class StreamData : WebSocketDelegate {
    
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
    
    func writeEntireMotionData(dict: [String:[Double]]){
        guard let json = try? JSONSerialization.data(withJSONObject: dict) else {
            print("JSON encoding failed")
            return
        } //convert to json bytes
        self.socket.write(data: json, completion: .none)
    }
    
    func writeSingleMotionData(pitch: Double, roll: Double, yaw: Double){
        let cur_data_packet = ["pitch": pitch, "roll": roll, "yaw": yaw]
        
        guard let json = try? JSONSerialization.data(withJSONObject: cur_data_packet) else {
            print("JSON encoding failed")
            return
        }
        
        self.socket.write(data: json, completion: .none)
    }
    
    //takes in any dict of modeling motion data and location data attributes, convert to json and stream without enforcing schema checks.
    //TODO: think about if we need to enforce schema checks?
    func writeCombinedMotionLocationData(motion_data: [String:Double], location_data: [String:Double], stream_key: String){
        
        //combine 2 dicts
        let merged_dict = motion_data.merging(location_data) { (motionVals, locationVals) in
            return motionVals + locationVals
        }
        // Insert the stream_key alongside numeric fields
        var payload: [String: Any] = merged_dict
        payload["stream_key"] = stream_key

        //conv to json
        guard let json = try? JSONSerialization.data(withJSONObject: payload) else {
            print("JSON encoding failed")
            return}
        
        self.socket.write(data: json, completion: .none)
        
        }
    
    func websocketDisconnect(){
        self.socket.disconnect()
    }
}
