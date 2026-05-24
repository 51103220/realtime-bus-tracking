package com.bustrack.functions;

import com.bustrack.model.BusEvent;
import com.bustrack.model.InvalidBusEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// kiểm tra tọa độ và tốc độ — dùng CoordinateValidatorProcess để có side output
public class CoordinateValidator extends RichFlatMapFunction<BusEvent, BusEvent> {

    public static final OutputTag<InvalidBusEvent> INVALID_TAG =
            new OutputTag<InvalidBusEvent>("invalid-events") {};

    // bounding box TPHCM
    private static final double LON_MIN = 106.4;
    private static final double LON_MAX = 107.1;
    private static final double LAT_MIN = 10.3;
    private static final double LAT_MAX = 11.2;
    private static final double MAX_HARDWARE_SPEED = 120.0;

    private transient Counter validCounter;
    private transient Counter invalidCounter;

    @Override
    public void open(Configuration cfg) {
        validCounter   = getRuntimeContext().getMetricGroup().counter("bustrack.coords.valid");
        invalidCounter = getRuntimeContext().getMetricGroup().counter("bustrack.coords.invalid");
    }

    @Override
    public void flatMap(BusEvent event, Collector<BusEvent> out) {
        String reason = validate(event);
        if (reason != null) {
            invalidCounter.inc();
            return;
        }
        validCounter.inc();
        out.collect(event);
    }

    public static String validate(BusEvent e) {
        if (e.x == null || e.y == null)               return "NULL_COORDS";
        if (e.x < LON_MIN || e.x > LON_MAX)           return "LON_OUT_OF_BOUNDS";
        if (e.y < LAT_MIN || e.y > LAT_MAX)           return "LAT_OUT_OF_BOUNDS";
        if (e.speed != null && e.speed < 0)            return "NEGATIVE_SPEED";
        if (e.speed != null && e.speed > MAX_HARDWARE_SPEED) return "HARDWARE_FAULT_SPEED";
        return null;
    }
}
