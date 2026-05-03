package com.bustrack.model;

import java.io.Serializable;

public class InvalidBusEvent implements Serializable {
    public BusEvent  event;
    public String    reason;
    public long      detectedAt;

    public InvalidBusEvent() {}

    public InvalidBusEvent(BusEvent event, String reason, long detectedAt) {
        this.event       = event;
        this.reason      = reason;
        this.detectedAt  = detectedAt;
    }
}
