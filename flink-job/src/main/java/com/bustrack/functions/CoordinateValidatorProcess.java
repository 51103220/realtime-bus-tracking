package com.bustrack.functions;

import com.bustrack.model.BusEvent;
import com.bustrack.model.InvalidBusEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// wrapper ProcessFunction để có side output cho event lỗi
public class CoordinateValidatorProcess extends ProcessFunction<BusEvent, BusEvent> {

    public static final OutputTag<InvalidBusEvent> INVALID_TAG =
            new OutputTag<InvalidBusEvent>("invalid-events") {};

    private transient Counter validCounter;
    private transient Counter invalidCounter;

    @Override
    public void open(Configuration cfg) {
        validCounter   = getRuntimeContext().getMetricGroup().counter("bustrack.coords.valid");
        invalidCounter = getRuntimeContext().getMetricGroup().counter("bustrack.coords.invalid");
    }

    @Override
    public void processElement(BusEvent event, Context ctx, Collector<BusEvent> out) {
        String reason = CoordinateValidator.validate(event);
        if (reason != null) {
            invalidCounter.inc();
            ctx.output(INVALID_TAG, new InvalidBusEvent(event, reason, System.currentTimeMillis()));
            return;
        }
        validCounter.inc();
        out.collect(event);
    }
}
