package com.bustrack.functions;

import com.bustrack.model.AnomalyEvent;
import com.bustrack.model.BusEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

// phát hiện bất thường GPS theo từng xe
public class AnomalyDetector extends KeyedProcessFunction<String, BusEvent, BusEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);

    public static final OutputTag<AnomalyEvent> ANOMALY_TAG =
            new OutputTag<AnomalyEvent>("anomaly-events") {};

    // ngưỡng phát hiện
    private static final double SPEED_EXCESS_KMH    = 80.0;
    private static final double GPS_JUMP_DIST_KM    = 0.5;
    private static final long   GPS_JUMP_MAX_SECS   = 30L;

    private transient ValueState<BusEvent> prevEventState;

    @Override
    public void open(Configuration cfg) {
        prevEventState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("prev-event", TypeInformation.of(BusEvent.class)));
    }

    @Override
    public void processElement(BusEvent curr, Context ctx, Collector<BusEvent> out) throws Exception {
        BusEvent prev = prevEventState.value();
        prevEventState.update(curr);

        if (prev == null) {
            out.collect(curr);
            return;
        }

        List<String> reasons = new ArrayList<>();

        // tốc độ quá cao
        if (curr.speed != null && curr.speed > SPEED_EXCESS_KMH) {
            reasons.add("SPEED_EXCESS:" + String.format("%.1f", curr.speed));
        }

        // xe nhảy vọt vị trí
        if (prev.x != null && prev.y != null && curr.x != null && curr.y != null) {
            double distKm = haversineKm(prev.y, prev.x, curr.y, curr.x);
            long   dtSecs = curr.datetime - prev.datetime;
            if (dtSecs > 0 && dtSecs <= GPS_JUMP_MAX_SECS && distKm > GPS_JUMP_DIST_KM) {
                reasons.add(String.format("GPS_JUMP:%.2fkm_in_%ds", distKm, dtSecs));
            }
        }

        if (!reasons.isEmpty()) {
            String type = String.join("|", reasons);
            ctx.output(ANOMALY_TAG, new AnomalyEvent(curr, type, prev));
        }

        out.collect(curr);
    }

    static double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6371.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                 + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                 * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
