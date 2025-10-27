//
//  LocationProcess.swift
//  coremotion_streaming
//
//  Created by Hanson Li on 10/27/25.
//

import Foundation
import CoreLocation
import Combine

// CoreLocation quick start: https://dwirandyh.medium.com/deep-dive-into-core-location-in-ios-a-step-by-step-guide-to-requesting-and-utilizing-user-location-fe8325462ea9 |
// need to add 'Privacy - Location when in Use Usage Description' (for actively using app) and 'Privacy - Location Always and when in Use Usage Description' (for background location data streaming) set in the coremotion_streaming target's info tab

// also need to add background mode with location updates enabled in the capabilities tab of target

//observableobject for dynamic swiftUI updates, CLLocationManagerDelegate because need to take return values from startLocationUpdate, NSObject so we can implement delegate for corelocation
final class LocationProcess: NSObject, ObservableObject, CLLocationManagerDelegate {
    
    private let locationManager = CLLocationManager()
    @Published var long: Double? = nil
    @Published var lat: Double? = nil
    @Published var loc_horizontal_acc: Double? = nil
    
    override init() {
        super.init()
        locationManager.delegate = self
        locationManager.desiredAccuracy = kCLLocationAccuracyBest

        if CLLocationManager.locationServicesEnabled() {
            locationManager.requestWhenInUseAuthorization() //when in use just for testing, change to always in use if we want to stream location 24/7. For always allow need when in use auth first
        }else{
            print("Location Service is not enabled!")
        }
                
    }
    
    //startUpdatingLocation for streaming. Uses up battery so be careful with background streaming
    func requestLocationUpdate() {
        locationManager.startUpdatingLocation() //returns to CLLocationManagerDelegate function, which needs to be implemenetd
    }
    
    func stopLocationUpdate() {
        locationManager.stopUpdatingLocation()
    }

    
    func locationManager(_ manager: CLLocationManager, didUpdateLocations locations: [CLLocation]) {
        guard let location = locations.last else { return }
        self.long = location.coordinate.longitude
        self.lat = location.coordinate.latitude
        self.loc_horizontal_acc = location.horizontalAccuracy
        print("Latitude: \(self.long), Longitude: \(self.lat)")
    }
    
    
    //error delegate also needed
    func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {
            locationManager.stopUpdatingLocation()

            if let clErr = error as? CLError {
                switch clErr.code {
                case .locationUnknown, .denied, .network:
                    print("Location request failed with error: \(clErr.localizedDescription)")
                case .headingFailure:
                    print("Heading request failed with error: \(clErr.localizedDescription)")
                case .rangingUnavailable, .rangingFailure:
                    print("Ranging request failed with error: \(clErr.localizedDescription)")
                case .regionMonitoringDenied, .regionMonitoringFailure, .regionMonitoringSetupDelayed, .regionMonitoringResponseDelayed:
                    print("Region monitoring request failed with error: \(clErr.localizedDescription)")
                default:
                    print("Unknown location manager error: \(clErr.localizedDescription)")
                }
            } else {
                print("Unknown error occurred while handling location manager error: \(error.localizedDescription)")
            }
        }
    
    
}
