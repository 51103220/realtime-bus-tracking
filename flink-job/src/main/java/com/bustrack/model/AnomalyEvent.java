package com.bustrack.model;

import java.io.Serializable;

public class AnomalyEvent implements Serializable {
    public String vehicle;
    public String routeNo;
    public long   datetime;
    public String anomalyType;     // e.g. "SPEED_EXCESS:95.0" or "GPS_JUMP:1.2km"
    public Double speed;
    public Double lon;
    public Double lat;
    public Double prevLon;
    public Double prevLat;
    public double distanceFromPrevKm;
    public long   secondsFromPrev;

    public AnomalyEvent() {}

    public AnomalyEvent(BusEvent curr, String anomalyType, BusEvent prev) {
        this.vehicle         = curr.vehicle;
        this.routeNo         = curr.routeNo;
        this.datetime        = curr.datetime;
        this.anomalyType     = anomalyType;
        this.speed           = curr.speed;
        this.lon             = curr.x;
        this.lat             = curr.y;
        this.prevLon         = prev != null ? prev.x : null;
        this.prevLat         = prev != null ? prev.y : null;
        this.secondsFromPrev = prev != null ? curr.datetime - prev.datetime : 0;
        if (prev != null && curr.x != null && curr.y != null && prev.x != null && prev.y != null) {
            this.distanceFromPrevKm = haversineKm(prev.y, prev.x, curr.y, curr.x);
        }
    }

    private static double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6371.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                 + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                 * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
