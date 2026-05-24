package com.bustrack.functions;

import com.bustrack.model.BusEvent;
import com.bustrack.model.VehicleFeatureVector;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

// trích 12 feature mỗi xe mỗi 1h — đầu vào cho PCA offline
public class VehicleFeatureExtractor
        extends ProcessWindowFunction<BusEvent, VehicleFeatureVector, String, TimeWindow> {

    private static final double IDLE_SPEED_KMH = 2.0;
    private static final double HEADING_CHANGE_DEG = 45.0;

    @Override
    public void process(String vehicle, Context ctx, Iterable<BusEvent> events, Collector<VehicleFeatureVector> out) {
        List<BusEvent> sorted = new ArrayList<>();
        events.forEach(sorted::add);
        if (sorted.isEmpty()) return;

        sorted.sort((a, b) -> Long.compare(a.datetime, b.datetime));

        int hourOfDay = Instant.ofEpochMilli(ctx.window().getStart())
                .atZone(ZoneOffset.ofHours(7)) // giờ Việt Nam
                .getHour();

        // tốc độ
        List<Double> speeds = new ArrayList<>();
        int idleCount = 0;
        for (BusEvent e : sorted) {
            if (e.speed != null) {
                speeds.add(e.speed);
                if (e.speed < IDLE_SPEED_KMH) idleCount++;
            }
        }
        double avgSpeed = speeds.stream().mapToDouble(d -> d).average().orElse(0);
        double maxSpeed = speeds.stream().mapToDouble(d -> d).max().orElse(0);
        double stdSpeed = stdDev(speeds, avgSpeed);
        double idleFraction = sorted.isEmpty() ? 0 : (double) idleCount / sorted.size();

        // khoảng cách
        double totalDist = 0;
        for (int i = 1; i < sorted.size(); i++) {
            BusEvent p = sorted.get(i - 1), c = sorted.get(i);
            if (p.x != null && p.y != null && c.x != null && c.y != null) {
                totalDist += haversineKm(p.y, p.x, c.y, c.x);
            }
        }

        // độ đều của sampling
        List<Long> intervals = new ArrayList<>();
        for (int i = 1; i < sorted.size(); i++) {
            intervals.add(sorted.get(i).datetime - sorted.get(i - 1).datetime);
        }
        List<Double> intervalD = new ArrayList<>();
        intervals.forEach(l -> intervalD.add((double) l));
        double avgInterval = intervalD.stream().mapToDouble(d -> d).average().orElse(0);
        double stdInterval = stdDev(intervalD, avgInterval);

        // số lần rẽ
        int headingChanges = 0;
        for (int i = 1; i < sorted.size(); i++) {
            Double h1 = sorted.get(i - 1).heading;
            Double h2 = sorted.get(i).heading;
            if (h1 != null && h2 != null) {
                double delta = Math.abs(h2 - h1);
                if (delta > 180) delta = 360 - delta; // góc nhỏ nhất
                if (delta > HEADING_CHANGE_DEG) headingChanges++;
            }
        }

        // tỉ lệ bật máy / điều hòa
        long ignitionOn = sorted.stream().filter(e -> Boolean.TRUE.equals(e.ignition)).count();
        long airconOn   = sorted.stream().filter(e -> Boolean.TRUE.equals(e.aircon)).count();

        // ghép vector
        VehicleFeatureVector vec = new VehicleFeatureVector();
        vec.vehicle                = vehicle;
        vec.routeNo                = sorted.get(0).routeNo;
        vec.windowStartMs          = ctx.window().getStart();
        vec.hourOfDay              = hourOfDay;
        vec.eventCount             = sorted.size();
        vec.avgSpeed               = avgSpeed;
        vec.maxSpeed               = maxSpeed;
        vec.stdSpeed               = stdSpeed;
        vec.idleFraction           = idleFraction;
        vec.totalDistanceKm        = totalDist;
        vec.avgSamplingIntervalS   = avgInterval;
        vec.samplingIrregularity   = stdInterval;
        vec.hourSin                = Math.sin(2 * Math.PI * hourOfDay / 24.0);
        vec.hourCos                = Math.cos(2 * Math.PI * hourOfDay / 24.0);
        vec.headingChanges         = headingChanges;
        vec.ignitionOnFraction     = sorted.isEmpty() ? 0 : (double) ignitionOn / sorted.size();
        vec.airconOnFraction       = sorted.isEmpty() ? 0 : (double) airconOn   / sorted.size();

        out.collect(vec);
    }

    private static double stdDev(List<Double> vals, double mean) {
        if (vals.size() < 2) return 0;
        double variance = vals.stream().mapToDouble(v -> (v - mean) * (v - mean)).sum() / vals.size();
        return Math.sqrt(variance);
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
