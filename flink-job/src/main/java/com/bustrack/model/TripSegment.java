package com.bustrack.model;

import java.io.Serializable;

public class TripSegment implements Serializable {
    public String vehicle;
    public String routeNo;
    public String routeId;
    public long   tripStart;        // unix seconds
    public long   tripEnd;
    public long   durationSeconds;
    public int    eventCount;
    public double totalDistanceKm;
    public double avgSpeedKmh;
    public int    stopCount;        // events where speed < 2 km/h
    public double startLon;
    public double startLat;
    public double endLon;
    public double endLat;

    public TripSegment() {}
}
