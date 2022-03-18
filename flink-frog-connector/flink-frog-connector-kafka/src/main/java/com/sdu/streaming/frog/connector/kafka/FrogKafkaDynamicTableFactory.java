package com.sdu.streaming.frog.connector.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class FrogKafkaDynamicTableFactory extends KafkaDynamicTableFactory {

    private static final String IDENTIFIER = "frog-kafka";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected KafkaDynamicSource createKafkaTableSource(DataType physicalDataType,
                                                        @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
                                                        DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
                                                        int[] keyProjection,
                                                        int[] valueProjection,
                                                        @Nullable String keyPrefix,
                                                        @Nullable List<String> topics,
                                                        @Nullable Pattern topicPattern,
                                                        Properties properties,
                                                        StartupMode startupMode,
                                                        Map<KafkaTopicPartition, Long> specificStartupOffsets,
                                                        long startupTimestampMillis,
                                                        String tableIdentifier) {
        // TODO: support get kafka topic partition
        return new FrogKafkaDynamicSource(physicalDataType,
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
                false,
                tableIdentifier,
                1);
    }

}
