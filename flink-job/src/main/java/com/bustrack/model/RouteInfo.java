package com.bustrack.model;

import java.io.Serializable;

public class RouteInfo implements Serializable {
    public final String routeId;
    public final String routeNo;

    public RouteInfo(String routeId, String routeNo) {
        this.routeId = routeId;
        this.routeNo = routeNo;
    }
}
