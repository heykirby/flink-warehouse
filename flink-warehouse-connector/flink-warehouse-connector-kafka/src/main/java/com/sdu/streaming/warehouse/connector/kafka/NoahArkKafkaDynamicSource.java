package com.sdu.streaming.warehouse.connector.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class NoahArkKafkaDynamicSource extends KafkaDynamicSource {

    private final int parallelism;

    public NoahArkKafkaDynamicSource(DataType physicalDataType,
                                     DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
                                     DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
                                     int[] keyProjection,
                                     int[] valueProjection,
                                     String keyPrefix,
                                     List<String> topics,
                                     Pattern topicPattern,
                                     Properties properties,
                                     StartupMode startupMode,
                                     Map<KafkaTopicPartition, Long> specificStartupOffsets,
                                     long startupTimestampMillis,
                                     boolean upsertMode,
                                     String tableIdentifier,
                                     int parallelism) {
        super(physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topics,
                topicPattern,
                properties,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                upsertMode,
                tableIdentifier);
        this.parallelism = parallelism;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        // TODO: supported source parallelism
        return super.getScanRuntimeProvider(context);
    }
}
