package com.bustrack.model;

import java.io.Serializable;

public class BusEvent implements Serializable {

    // trường GPS thô
    public String vehicle;
    public String driver;
    public Long datetime;
    public Double x;            // kinh độ
    public Double y;            // vĩ độ
    public Double speed;        // có thể null
    public Double heading;      // có thể null
    public Boolean ignition;
    public Boolean aircon;
    public Boolean working;
    public Boolean doorUp;      // có thể null
    public Boolean doorDown;    // có thể null

    // trường được gắn thêm bởi BusDeduplicator
    public String routeId;
    public String routeNo;

    public BusEvent() {}

    public String dedupKey() {
        return vehicle + "|" + datetime;
    }
}
