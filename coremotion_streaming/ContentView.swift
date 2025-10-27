//
//  ContentView.swift
//  coremotion_streaming
//
//  Created by Hanson Li on 10/27/25.
//

import SwiftUI

struct ContentView: View {
    @StateObject private var motion = MotionProcess()

    var body: some View {
        VStack(spacing: 16) {
            Text("📱 CoreMotion Stream")
                .font(.title2)
                .bold()

            if let pitch = motion.motionData["pitch"]?.last,
               let pitch_accl = motion.motionData["accl_x"]?.last,
               let roll = motion.motionData["roll"]?.last,
               let yaw = motion.motionData["yaw"]?.last {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Pitch: \(pitch, specifier: "%.3f")")
                    Text("Pitch Accl: \(pitch_accl, specifier: "%.2f")")
                    Text("Roll:  \(roll,  specifier: "%.3f")")
                    Text("Yaw:   \(yaw,   specifier: "%.3f")")
                }
                .font(.system(.body, design: .monospaced))
            } else {
                Text("Waiting for motion data…")
                    .foregroundColor(.secondary)
            }

            Button(action: {
                motion.getDeviceMotion()
            }) {
                Text("Start Motion Updates")
                    .padding()
                    .background(Color.blue.opacity(0.2))
                    .cornerRadius(10)
            }

            Button(action: {
                motion.stopGetMotion()
            }) {
                Text("Stop")
                    .padding()
                    .background(Color.red.opacity(0.2))
                    .cornerRadius(10)
            }
        }
        .padding()
    }
}


#Preview {
    ContentView()
}
