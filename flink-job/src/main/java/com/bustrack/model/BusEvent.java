package com.bustrack.model;

import java.io.Serializable;

public class BusEvent implements Serializable {

    // ── Raw GPS fields ─────────────────────────────────────────────────────
    public String vehicle;
    public String driver;
    public Long datetime;
    public Double x;            // longitude
    public Double y;            // latitude
    public Double speed;        // optional
    public Double heading;      // optional
    public Boolean ignition;
    public Boolean aircon;
    public Boolean working;
    public Boolean doorUp;      // optional
    public Boolean doorDown;    // optional

    // ── Enriched fields (populated by BusDeduplicator) ────────────────────
    public String routeId;
    public String routeNo;

    public BusEvent() {}

    public String dedupKey() {
        return vehicle + "|" + datetime;
    }
}
