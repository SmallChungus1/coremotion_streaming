//
//  CombineMotionLocationStream.swift
//  coremotion_streaming
//
//  Created by Hanson Li on 10/27/25.
//

import Foundation
import Combine



//logic ontop of StreamData to monitor for changes from Motion and Location streams from LocationProcess and MotionProcess,then call streamer's writeCombinedMotionLocationData method to stream the new data

final class CombineMotionLocationStream: ObservableObject {
    
    private var cancellables = Set<AnyCancellable>()
    private let streamer = StreamData()
    
    init(motion: MotionProcess, location: LocationProcess) {
        // CombineLatest merges new motion data and location data. Can combine up to 4 publisher using CombineLatestX, where x <= 4. If x > 4, use commented code block below
        
        Publishers.CombineLatest4(motion.$motionData, location.$cl_speed, location.$lat, location.$long)
        // for comibing latest > 4 publishers:
//        Publishers.CombineLatest(
//            motion.$motionData,
//            Publishers.CombineLatest(location.$lat, Publishers.CombineLatest(location.$long, location.$cl_speed))
//        )
        
        .receive(on: DispatchQueue.main) //DispatchQueue.main ensure it runs on the main thread, helpful for thread saftey and updating swiftui states
        .sink { motionData, speed, lat, lon in//.sink subscribes to the motion and location publishers
            
            guard let pitch = motionData["pitch"]?.last,
                  let roll = motionData["roll"]?.last,
                  let yaw = motionData["yaw"]?.last,
                  let lat = lat,
                  let lon = lon,
                  let speed = speed else {
                return
            }
            
            let motion_dict = ["pitch": pitch, "roll": roll, "yaw": yaw]
            let location_dict = ["lat": lat, "lon": lon, "speed": speed]
            
            //call the stream method
            self.streamer.writeCombinedMotionLocationData(
                motion_data: motion_dict,
                location_data: location_dict
            )
        }
        .store(in: &cancellables) //need this to keep subscriptions alive as long as method class instance is alive
    }
}


//final class CombineMotionLocationStream: ObservableObject {
//    
//    private var cancellables = Set<AnyCancellable>()
//    private let streamer = StreamData()
//    
//    init(motion: MotionProcess, location: LocationProcess) {
//        // CombineLatest merges new motion data and location data
//        Publishers.CombineLatest(
//            motion.$motionData,
//            Publishers.CombineLatest(location.$lat, Publishers.CombineLatest(location.$long, location.$loc_horizontal_acc))
//        )
//        .receive(on: DispatchQueue.main) //DispatchQueue.main ensure it runs on the main thread, helpful for thread saftey and updating swiftui states
//        .sink { motionData, locationTuple in //.sink subscribes to the motion and location publishers
//            let (lat, (lon, acc)) = locationTuple
//            
//            guard let pitch = motionData["pitch"]?.last,
//                  let roll = motionData["roll"]?.last,
//                  let yaw = motionData["yaw"]?.last,
//                  let lat = lat,
//                  let lon = lon,
//                  let acc = acc else {
//                return
//            }
//            
//            let motion_dict = ["pitch": pitch, "roll": roll, "yaw": yaw]
//            let location_dict = ["lat": lat, "lon": lon, "horizontal_acc": acc]
//            
//            //call the stream method
//            self.streamer.writeCombinedMotionLocationData(
//                motion_data: motion_dict,
//                location_data: location_dict
//            )
//        }
//        .store(in: &cancellables) //need this to keep subscriptions alive as long as method class instance is alive
//    }
//}
