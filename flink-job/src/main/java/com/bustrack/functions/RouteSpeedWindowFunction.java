package com.bustrack.functions;

import com.bustrack.model.BusEvent;
import com.bustrack.model.RouteSpeedSnapshot;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Attaches window metadata (windowEndMs) to the aggregated RouteSpeedSnapshot
 * produced by SpeedAggregator. This two-function pattern (AggregateFunction +
 * ProcessWindowFunction) is the standard Flink approach for combining
 * incremental aggregation (O(1) memory) with window context access.
 */
public class RouteSpeedWindowFunction
        extends ProcessWindowFunction<RouteSpeedSnapshot, RouteSpeedSnapshot, String, TimeWindow> {

    @Override
    public void process(String routeNo, Context ctx, Iterable<RouteSpeedSnapshot> input, Collector<RouteSpeedSnapshot> out) {
        RouteSpeedSnapshot snap = input.iterator().next();
        snap.routeNo     = routeNo;
        snap.windowEndMs = ctx.window().getEnd();
        out.collect(snap);
    }
}
