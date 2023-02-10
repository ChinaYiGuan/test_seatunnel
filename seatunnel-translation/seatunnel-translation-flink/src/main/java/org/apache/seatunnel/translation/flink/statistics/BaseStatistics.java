package org.apache.seatunnel.translation.flink.statistics;

import org.apache.flink.types.Row;
import org.apache.seatunnel.api.common.PluginIdentifierInterface;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.StringJoiner;
import java.util.UUID;

public interface BaseStatistics extends Serializable {

    void open();

    void close();

    void statistics(Row row);

    <T extends PluginIdentifierInterface> T getPlugin();

    Config getConfig();

    default String getStatisticsId() {
        StringJoiner joiner = new StringJoiner("_");
        joiner.add(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss.SSS").format(LocalDateTime.now()));
        joiner.add(UUID.randomUUID().toString().replace("-", ""));
        joiner.add(this.hashCode() + "");
        return joiner.toString();

    }
}
