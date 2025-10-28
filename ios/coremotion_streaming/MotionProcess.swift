//
//  MotionProcess.swift
//  coremotion_streaming
//
//  Created by Hanson Li on 10/27/25.
//

//medium/github ref: https://medium.com/appledeveloperacademy-ufpe/swift-how-to-use-coremotion-to-obtain-sensorial-data-20b1b73a948a /  https://github.com/thaxz/CoreMotionTutorial/blob/main/CoreMotionTutorial/ViewModel/HomeViewModel.swift

//apple setup doc: https://developer.apple.com/documentation/coremotion/getting-processed-device-motion-data

import Foundation
import CoreMotion
import Combine

class MotionProcess: ObservableObject {
    //private var streamer = StreamData()
    
    //publisher delcaration, which emits (to its subscribers) everytime data (in motionData) changes
    @Published var motionData: [String: [Double]] = [:]
    private var motion: CMMotionManager!
    private var timer: Timer!
    
    init() {
        self.motion = CMMotionManager()
    }
    
    
    //keep this in mind for timer-free approach:
//    motion.startDeviceMotionUpdates(to: .main) { data, error in
//        guard let data = data else { return }
//        let attitude = data.attitude
//        let accel = data.userAcceleration
//
//        self.motionData["pitch", default: []].append(attitude.pitch)
//        self.motionData["roll", default: []].append(attitude.roll)
//        self.motionData["yaw", default: []].append(attitude.yaw)
//        self.motionData["accl_x", default: []].append(accel.x)
//        self.motionData["accl_y", default: []].append(accel.y)
//        self.motionData["accl_z", default: []].append(accel.z)
//    }
    
    //gets motion without polling
    func getDeviceMotion() {
        guard motion.isDeviceMotionAvailable else { return }
        
        motion.deviceMotionUpdateInterval = 1.0 / 5.0 // with x/y, it udpates y many times in x seconds so 1/20 is 20 hz

        motion.startDeviceMotionUpdates(using: .xMagneticNorthZVertical, to: .main) { data, error in
            guard let data = data else { return }
            let a = data.attitude
            let acc = data.userAcceleration

            self.motionData["pitch", default: []].append(a.pitch)
            self.motionData["roll", default: []].append(a.roll)
            self.motionData["yaw", default: []].append(a.yaw)
            self.motionData["accl_x", default: []].append(acc.x)
            self.motionData["accl_y", default: []].append(acc.y)
            self.motionData["accl_z", default: []].append(acc.z)
        }
    }

    
    //gets motion from device based on some set frequency, with manual polling
    func getDeviceMotionPolling() {
        if motion.isDeviceMotionAvailable {
            
            self.motion.deviceMotionUpdateInterval = 1.0 / 20.0 // with x/y, it udpates y many times in x seconds
            self.motion.startDeviceMotionUpdates(using: .xMagneticNorthZVertical) //start motion updates using reference frame
            self.timer = Timer(fire: Date(), interval: (1.0 / 20.0), repeats: true,
               block: { (timer) in
                if let data = self.motion.deviceMotion {
                    // Get the attitude relative to the magnetic north reference frame.
                    let a = data.attitude
                    let acc = data.userAcceleration

                    self.motionData["pitch", default: []].append(a.pitch)
                    self.motionData["roll", default: []].append(a.roll)
                    self.motionData["yaw", default: []].append(a.yaw)
                    self.motionData["accl_x", default: []].append(acc.x)
                    self.motionData["accl_y", default: []].append(acc.y)
                    self.motionData["accl_z", default: []].append(acc.z)
                    
                    //self.streamer.writeSingleMotionData(pitch: a.pitch, roll: a.roll, yaw: a.yaw)
                    
                }
            })
            
            // Add the timer to the current run loop.
            RunLoop.current.add(self.timer!, forMode: RunLoop.Mode.default)
            
        }
    }
    
    func stopGetMotion() {
        motion.stopDeviceMotionUpdates()
    }
}
