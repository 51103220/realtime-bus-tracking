package com.bustrack.functions;

import com.bustrack.model.BusEvent;
import com.bustrack.model.RouteSpeedSnapshot;
import org.apache.flink.api.common.functions.AggregateFunction;

// tính tốc độ trung bình/min/max theo cửa sổ trượt — O(1) memory
public class SpeedAggregator
        implements AggregateFunction<BusEvent, SpeedAggregator.Acc, RouteSpeedSnapshot> {

    public static class Acc {
        String routeNo;
        double speedSum   = 0;
        double speedMin   = Double.MAX_VALUE;
        double speedMax   = Double.MIN_VALUE;
        int    speedCount = 0;
        int    eventCount = 0;
        // đếm event, không phải unique vehicle
        int    vehicleCount = 0;
    }

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    @Override
    public Acc add(BusEvent event, Acc acc) {
        acc.routeNo = event.routeNo;
        acc.eventCount++;
        acc.vehicleCount++; // đơn giản hóa: đếm event chứ không unique vehicle
        if (event.speed != null) {
            acc.speedSum += event.speed;
            acc.speedCount++;
            if (event.speed < acc.speedMin) acc.speedMin = event.speed;
            if (event.speed > acc.speedMax) acc.speedMax = event.speed;
        }
        return acc;
    }

    @Override
    public RouteSpeedSnapshot getResult(Acc acc) {
        RouteSpeedSnapshot snap = new RouteSpeedSnapshot();
        snap.routeNo      = acc.routeNo;
        snap.eventCount   = acc.eventCount;
        snap.vehicleCount = acc.vehicleCount;
        snap.avgSpeedKmh  = acc.speedCount > 0 ? acc.speedSum / acc.speedCount : 0;
        snap.minSpeedKmh  = acc.speedMin == Double.MAX_VALUE ? 0 : acc.speedMin;
        snap.maxSpeedKmh  = acc.speedMax == Double.MIN_VALUE ? 0 : acc.speedMax;
        return snap;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        a.routeNo      = a.routeNo != null ? a.routeNo : b.routeNo;
        a.speedSum    += b.speedSum;
        a.speedCount  += b.speedCount;
        a.eventCount  += b.eventCount;
        a.vehicleCount += b.vehicleCount;
        if (b.speedMin < a.speedMin) a.speedMin = b.speedMin;
        if (b.speedMax > a.speedMax) a.speedMax = b.speedMax;
        return a;
    }
}
