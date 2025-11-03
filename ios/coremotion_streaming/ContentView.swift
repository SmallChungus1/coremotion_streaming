//
//  ContentView.swift
//  coremotion_streaming
//
//  Created by Hanson Li on 10/27/25.
//

import SwiftUI
import UIKit

struct ContentView: View {
    @StateObject private var motion = MotionProcess()
    @StateObject private var location = LocationProcess()
    @StateObject private var combinedStream: CombineMotionLocationStream
    let deviceName = UIDevice.current.name
    let isoFormatter = ISO8601DateFormatter()
    
    @State private var streamStartTime: String? = nil //only updated when you press 'Start Motion' button, used as part of the key to identify stream sessions

    var streamKey: String? {
        guard let start = streamStartTime else { return nil }
        return "\(deviceName)_\(start)"
    }

    
    // Custom initializer to set up combined stream after motion and location
    init() {
        let motion = MotionProcess()
        let location = LocationProcess()
        _motion = StateObject(wrappedValue: motion)
        _location = StateObject(wrappedValue: location)
        _combinedStream = StateObject(wrappedValue: CombineMotionLocationStream(motion: motion, location: location))
    }
    
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
                       let yaw = motion.motionData["yaw"]?.last,
                       let lat = location.lat,
                       let lon = location.long,
                       let cl_speed = location.cl_speed{
                        Text("Pitch: \(pitch, specifier: "%.3f")")
                        Text("Roll:  \(roll, specifier: "%.3f")")
                        Text("Yaw:   \(yaw, specifier: "%.3f")")
                        Text("Latitude:  \(lat, specifier: "%.6f")")
                        Text("Longitude: \(lon, specifier: "%.6f")")
                        Text("speed: \(cl_speed, specifier: "%.5f")")
                    } else {
                        Text("No Motion or Location Data Yet")
                            .foregroundColor(.secondary)
                    }
                    
                    HStack {
                        Button("Start Motion") {
                            
                            // Capture the start time exactly once when starting
                            streamStartTime = isoFormatter.string(from: Date())

                            motion.getDeviceMotion()
                            location.requestLocationUpdate()  //note that location streaming will start automatically due
                        }
                        Button("Stop") {
                            motion.stopGetMotion()
                            location.stopLocationUpdate()
                            
                        }
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
