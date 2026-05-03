package com.bustrack.model;

import java.io.Serializable;

public class RouteSpeedSnapshot implements Serializable {
    public String routeNo;
    public long   windowEndMs;
    public double avgSpeedKmh;
    public double minSpeedKmh;
    public double maxSpeedKmh;
    public int    vehicleCount;
    public int    eventCount;

    public RouteSpeedSnapshot() {}
}
