package com.bustrack.model;

import java.io.Serializable;

/**
 * 12-dimensional feature vector per vehicle per hour.
 * Written to MinIO features/vehicle-hourly/ as JSON input for offline PCA.
 */
public class VehicleFeatureVector implements Serializable {
    public String vehicle;
    public String routeNo;
    public long   windowStartMs;
    public int    hourOfDay;        // 0-23

    // Speed features
    public double eventCount;
    public double avgSpeed;         // mean of non-null speed values
    public double maxSpeed;
    public double stdSpeed;         // standard deviation
    public double idleFraction;     // fraction where speed < 2 km/h

    // Spatial features
    public double totalDistanceKm;  // sum of haversine distances

    // Temporal regularity features
    public double avgSamplingIntervalS;   // mean seconds between consecutive events
    public double samplingIrregularity;   // std dev of sampling intervals

    // Cyclic hour encoding (avoids midnight discontinuity)
    public double hourSin;          // sin(2π * hourOfDay / 24)
    public double hourCos;          // cos(2π * hourOfDay / 24)

    // Operational features
    public double headingChanges;   // count of |heading_delta| > 45 degrees
    public double ignitionOnFraction;
    public double airconOnFraction;

    public VehicleFeatureVector() {}

    /** Returns the 12 core features as a double array for sklearn PCA input. */
    public double[] toArray() {
        return new double[]{
            eventCount, avgSpeed, maxSpeed, stdSpeed, idleFraction,
            totalDistanceKm, avgSamplingIntervalS, samplingIrregularity,
            hourSin, hourCos, headingChanges, ignitionOnFraction
        };
    }
}
