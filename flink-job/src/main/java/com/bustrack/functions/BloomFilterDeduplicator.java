package com.bustrack.functions;

import com.bustrack.model.BusEvent;
import com.bustrack.model.DedupMetrics;
import com.bustrack.model.RouteInfo;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

// dedup dùng bloom filter, tiết kiệm bộ nhớ hơn HashMap nhiều
public class BloomFilterDeduplicator extends KeyedProcessFunction<String, BusEvent, BusEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(BloomFilterDeduplicator.class);

    public static final OutputTag<DedupMetrics> METRICS_TAG =
            new OutputTag<DedupMetrics>("dedup-metrics") {};

    // FPP 1%, mỗi xe khoảng 10K event trong TTL là đủ
    private static final double FALSE_POSITIVE_RATE = 0.01;
    private static final int    EXPECTED_INSERTIONS  = 10_000;

    private final String routeMappingPath;
    private final int    dedupTtlSeconds;

    private transient ValueState<byte[]>  bloomState;       // bloom filter serialized
    private transient ValueState<Long>    totalSeenState;
    private transient ValueState<Long>    droppedState;
    private transient Map<String, RouteInfo> routeMap;

    public BloomFilterDeduplicator(String routeMappingPath, int dedupTtlSeconds) {
        this.routeMappingPath = routeMappingPath;
        this.dedupTtlSeconds  = dedupTtlSeconds;
    }

    @Override
    public void open(Configuration cfg) throws Exception {
        // TTL dài hơn để quản lý bộ nhớ, reset theo giờ
        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(Time.seconds(Math.max(dedupTtlSeconds * 10L, 3600L)))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<byte[]> bloomDesc = new ValueStateDescriptor<>("bloom-filter",
                org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        bloomDesc.enableTimeToLive(ttl);
        bloomState = getRuntimeContext().getState(bloomDesc);

        ValueStateDescriptor<Long> seenDesc = new ValueStateDescriptor<>("total-seen", Types.LONG);
        totalSeenState = getRuntimeContext().getState(seenDesc);

        ValueStateDescriptor<Long> dropDesc = new ValueStateDescriptor<>("total-dropped", Types.LONG);
        droppedState = getRuntimeContext().getState(dropDesc);

        routeMap = loadRouteMap(routeMappingPath);
        LOG.info("BloomFilterDeduplicator loaded {} route mappings", routeMap.size());
    }

    @Override
    public void processElement(BusEvent event, Context ctx, Collector<BusEvent> out) throws Exception {
        BloomFilter<String> bf = loadOrCreate();

        String key = event.vehicle + "|" + event.datetime;

        long seen    = totalSeenState.value() == null ? 0L : totalSeenState.value();
        long dropped = droppedState.value()    == null ? 0L : droppedState.value();
        seen++;

        if (bf.mightContain(key)) {
            // có thể trùng — bỏ qua
            dropped++;
            droppedState.update(dropped);
            totalSeenState.update(seen);

            // gửi metrics mỗi 5000 event để theo dõi
            if (seen % 5000 == 0) emitMetrics(ctx, bf, seen, dropped);
            return;
        }

        bf.put(key);
        bloomState.update(serialize(bf));
        totalSeenState.update(seen);
        droppedState.update(dropped);

        // gắn tuyến vào event
        RouteInfo route = routeMap.get(event.vehicle);
        if (route != null) {
            event.routeId = route.routeId;
            event.routeNo = route.routeNo;
        } else {
            event.routeNo = "UNKNOWN";
            event.routeId = "0";
        }

        if (seen % 5000 == 0) emitMetrics(ctx, bf, seen, dropped);
        out.collect(event);
    }

    private BloomFilter<String> loadOrCreate() throws Exception {
        byte[] bytes = bloomState.value();
        if (bytes == null || bytes.length == 0) {
            return BloomFilter.create(
                    Funnels.stringFunnel(StandardCharsets.UTF_8),
                    EXPECTED_INSERTIONS,
                    FALSE_POSITIVE_RATE);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            return BloomFilter.readFrom(bais, Funnels.stringFunnel(StandardCharsets.UTF_8));
        }
    }

    private byte[] serialize(BloomFilter<String> bf) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            bf.writeTo(baos);
            return baos.toByteArray();
        }
    }

    private void emitMetrics(Context ctx, BloomFilter<String> bf, long seen, long dropped) throws Exception {
        DedupMetrics m = new DedupMetrics();
        m.vehicle              = getCurrentKey();
        m.windowStartMs        = ctx.timerService().currentProcessingTime();
        m.totalSeen            = seen;
        m.duplicatesDropped    = dropped;
        m.duplicateRate        = seen > 0 ? (double) dropped / seen : 0.0;
        // Guava không có hàm lấy size trực tiếp, xấp xỉ qua serialized bytes
        m.bloomFilterSizeBytes = serialize(bf).length;
        m.numHashFunctions     = (int) Math.ceil(-Math.log(FALSE_POSITIVE_RATE) / Math.log(2));
        ctx.output(METRICS_TAG, m);
    }

    private String getCurrentKey() {
        try {
            return getRuntimeContext().getTaskNameWithSubtasks();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private Map<String, RouteInfo> loadRouteMap(String path) {
        Map<String, RouteInfo> map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            br.readLine(); // bỏ qua dòng tiêu đề
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",", -1);
                if (parts.length >= 3) {
                    map.put(parts[0].trim(), new RouteInfo(parts[1].trim(), parts[2].trim()));
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to load route mapping: {}", e.getMessage());
        }
        return map;
    }
}
