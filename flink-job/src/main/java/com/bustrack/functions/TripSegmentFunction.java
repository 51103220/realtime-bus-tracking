package com.bustrack.functions;

import com.bustrack.model.BusEvent;
import com.bustrack.model.TripSegment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

// session window — mỗi lần xe nghỉ 5 phút là tách thành một chuyến mới
public class TripSegmentFunction extends ProcessWindowFunction<BusEvent, TripSegment, String, TimeWindow> {

    private static final double STOP_SPEED_KMH = 2.0; // xe đứng yên

    @Override
    public void process(String vehicle, Context ctx, Iterable<BusEvent> events, Collector<TripSegment> out) {
        List<BusEvent> sorted = new ArrayList<>();
        events.forEach(sorted::add);
        if (sorted.isEmpty()) return;

        sorted.sort((a, b) -> Long.compare(a.datetime, b.datetime));

        BusEvent first = sorted.get(0);
        BusEvent last  = sorted.get(sorted.size() - 1);

        TripSegment seg = new TripSegment();
        seg.vehicle         = vehicle;
        seg.routeNo         = first.routeNo;
        seg.routeId         = first.routeId;
        seg.tripStart       = first.datetime;
        seg.tripEnd         = last.datetime;
        seg.durationSeconds = last.datetime - first.datetime;
        seg.eventCount      = sorted.size();
        seg.startLon        = first.x != null ? first.x : 0;
        seg.startLat        = first.y != null ? first.y : 0;
        seg.endLon          = last.x  != null ? last.x  : 0;
        seg.endLat          = last.y  != null ? last.y  : 0;

        // tính khoảng cách, tốc độ trung bình, số lần dừng
        double totalDist  = 0;
        double speedSum   = 0;
        int    speedCount = 0;
        int    stopCount  = 0;

        for (int i = 1; i < sorted.size(); i++) {
            BusEvent prev = sorted.get(i - 1);
            BusEvent curr = sorted.get(i);
            if (prev.x != null && prev.y != null && curr.x != null && curr.y != null) {
                totalDist += haversineKm(prev.y, prev.x, curr.y, curr.x);
            }
            if (curr.speed != null) {
                speedSum += curr.speed;
                speedCount++;
                if (curr.speed < STOP_SPEED_KMH) stopCount++;
            }
        }

        seg.totalDistanceKm = totalDist;
        seg.avgSpeedKmh     = speedCount > 0 ? speedSum / speedCount : 0;
        seg.stopCount       = stopCount;

        out.collect(seg);
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
