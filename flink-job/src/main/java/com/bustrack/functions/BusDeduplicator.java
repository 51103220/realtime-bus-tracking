package com.bustrack.functions;

import com.bustrack.model.BusEvent;
import com.bustrack.model.RouteInfo;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class BusDeduplicator extends KeyedProcessFunction<String, BusEvent, BusEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(BusDeduplicator.class);

    private final String routeMappingPath;
    private final int dedupTtlSeconds;

    // lưu thời điểm cuối cùng thấy xe này
    private transient ValueState<Long> lastDatetime;
    // map tuyến, load một lần thôi
    private transient Map<String, RouteInfo> routeMap;

    public BusDeduplicator(String routeMappingPath, int dedupTtlSeconds) {
        this.routeMappingPath = routeMappingPath;
        this.dedupTtlSeconds  = dedupTtlSeconds;
    }

    @Override
    public void open(Configuration cfg) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(dedupTtlSeconds))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>("last-datetime", Types.LONG);
        desc.enableTimeToLive(ttlConfig);
        lastDatetime = getRuntimeContext().getState(desc);

        routeMap = loadRouteMap(routeMappingPath);
        LOG.info("Loaded {} route mappings from {}", routeMap.size(), routeMappingPath);
    }

    @Override
    public void processElement(BusEvent event, Context ctx, Collector<BusEvent> out) throws Exception {
        Long last = lastDatetime.value();

        if (last != null && last.equals(event.datetime)) {
            // trùng — bỏ luôn
            return;
        }

        lastDatetime.update(event.datetime);

        // gắn tuyến
        RouteInfo route = routeMap.get(event.vehicle);
        if (route != null) {
            event.routeId = route.routeId;
            event.routeNo = route.routeNo;
        } else {
            event.routeNo = "UNKNOWN";
            event.routeId = "0";
        }

        out.collect(event);
    }

    private Map<String, RouteInfo> loadRouteMap(String path) {
        Map<String, RouteInfo> map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line = br.readLine(); // bỏ qua header: vehicle,route_id,route_no
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",", -1);
                if (parts.length >= 3) {
                    map.put(parts[0].trim(), new RouteInfo(parts[1].trim(), parts[2].trim()));
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to load route mapping from {}: {}", path, e.getMessage());
        }
        return map;
    }
}
