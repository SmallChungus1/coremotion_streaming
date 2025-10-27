//
//  ContentView.swift
//  coremotion_streaming
//
//  Created by Hanson Li on 10/27/25.
//

import SwiftUI

struct ContentView: View {
    @StateObject private var motion = MotionProcess()
    @StateObject private var location = LocationProcess()
    
    var body: some View {
        ScrollView {
            VStack(spacing: 24) {
                Text("CoreMotion + CoreLocation Stream")
                    .font(.title2)
                    .bold()
                
                VStack(alignment: .leading, spacing: 8) {
                    Text("CoreMotion Data")
                        .font(.headline)
                    if let pitch = motion.motionData["pitch"]?.last,
                       let roll = motion.motionData["roll"]?.last,
                       let yaw = motion.motionData["yaw"]?.last {
                        Text("Pitch: \(pitch, specifier: "%.3f")")
                        Text("Roll:  \(roll, specifier: "%.3f")")
                        Text("Yaw:   \(yaw, specifier: "%.3f")")
                    } else {
                        Text("No Motion Data Yet")
                            .foregroundColor(.secondary)
                    }
                    
                    HStack {
                        Button("Start Motion") { motion.getDeviceMotion() }
                        Button("Stop") { motion.stopGetMotion() }
                    }
                    .buttonStyle(.bordered)
                }
                
                VStack(alignment: .leading, spacing: 8) {
                    Text("CoreLocation Data")
                        .font(.headline)
                    if let lat = location.lat, let lon = location.long {
                        Text("Latitude:  \(lat, specifier: "%.5f")")
                        Text("Longitude: \(lon, specifier: "%.5f")")
                        if let acc = location.loc_horizontal_acc {
                            Text("Accuracy: ±\(acc, specifier: "%.1f") m")
                        }
                    } else {
                        Text("No Location Data Yet")
                            .foregroundColor(.secondary)
                    }
                    
                    HStack {
                        Button("Start Location") { location.requestLocationUpdate() }
                        Button("Stop") { location.stopLocationUpdate() }
                    }
                    .buttonStyle(.bordered)
                }
            }
            .padding()
        }
    }
}



#Preview {
    ContentView()
}
