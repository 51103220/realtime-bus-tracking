package com.bustrack.functions;

import com.bustrack.model.BusEvent;
import com.bustrack.model.TripSegment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Session window function that converts a gap-separated burst of events
 * from one vehicle into a TripSegment summary.
 *
 * Theory: Session windows close when no event arrives for a configurable gap
 * (here 5 minutes). This models real-world bus trips: a bus is idle at a
 * terminal for 5+ minutes between runs. Session windows are the only Flink
 * window type driven by data inactivity, not wall-clock time.
 *
 * Contrast with tumbling windows (fixed-duration, non-overlapping) and
 * sliding windows (fixed-duration, overlapping): session windows are
 * data-driven, which makes them semantically correct for trip extraction.
 */
public class TripSegmentFunction extends ProcessWindowFunction<BusEvent, TripSegment, String, TimeWindow> {

    private static final double STOP_SPEED_KMH = 2.0; // below this = bus stopped at stop

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

        // Compute total distance, avg speed, stop count
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
