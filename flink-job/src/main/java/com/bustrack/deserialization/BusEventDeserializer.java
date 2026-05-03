package com.bustrack.deserialization;

import com.bustrack.model.BusEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BusEventDeserializer implements KafkaRecordDeserializationSchema<BusEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(BusEventDeserializer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<BusEvent> out) throws IOException {
        if (record.value() == null) return;

        try {
            JsonNode root = MAPPER.readTree(record.value());
            JsonNode payload = root.path("msgBusWayPoint");

            if (payload.isMissingNode()) {
                LOG.warn("Missing msgBusWayPoint in record");
                return;
            }

            String vehicle = payload.path("vehicle").asText(null);
            if (vehicle == null || vehicle.isEmpty()) {
                LOG.warn("Missing vehicle field");
                return;
            }

            JsonNode datetimeNode = payload.path("datetime");
            if (datetimeNode.isMissingNode()) {
                LOG.warn("Missing datetime field for vehicle {}", vehicle);
                return;
            }

            BusEvent event = new BusEvent();
            event.vehicle  = vehicle;
            event.datetime = datetimeNode.asLong();
            event.x        = getDoubleOrNull(payload, "x");
            event.y        = getDoubleOrNull(payload, "y");
            event.speed    = getDoubleOrNull(payload, "speed");
            event.heading  = getDoubleOrNull(payload, "heading");
            event.driver   = payload.path("driver").asText(null);
            event.ignition = getBoolOrNull(payload, "ignition");
            event.aircon   = getBoolOrNull(payload, "aircon");
            event.working  = getBoolOrNull(payload, "working");
            event.doorUp   = getBoolOrNull(payload, "door_up");
            event.doorDown = getBoolOrNull(payload, "door_down");

            out.collect(event);
        } catch (Exception e) {
            LOG.warn("Failed to parse record: {}", e.getMessage());
        }
    }

    @Override
    public TypeInformation<BusEvent> getProducedType() {
        return TypeInformation.of(BusEvent.class);
    }

    private static Double getDoubleOrNull(JsonNode node, String field) {
        JsonNode n = node.path(field);
        return n.isMissingNode() || n.isNull() ? null : n.asDouble();
    }

    private static Boolean getBoolOrNull(JsonNode node, String field) {
        JsonNode n = node.path(field);
        return n.isMissingNode() || n.isNull() ? null : n.asBoolean();
    }
}
