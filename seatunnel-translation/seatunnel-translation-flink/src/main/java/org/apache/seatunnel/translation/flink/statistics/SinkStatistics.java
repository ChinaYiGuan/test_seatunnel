package org.apache.seatunnel.translation.flink.statistics;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

@Slf4j
@AllArgsConstructor
public class SinkStatistics implements BaseStatistics {

    private int totalCnt = 0;
    private Config sinkCfg;
    private SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sink;

    public SinkStatistics(Config sinkCfg, SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sink) {
        this.sinkCfg = sinkCfg;
        this.sink = sink;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() {
        log.info("【{}】The statistical value is:{} -> pluginName:{}", getStatisticsId(), totalCnt, sink.getPluginName());
    }

    @Override
    public void statistics(Row row) {
        totalCnt++;
    }

    @Override
    public SeaTunnelSink<SeaTunnelRow, ?, ?, ?> getPlugin() {
        return sink;
    }

    @Override
    public Config getConfig() {
        return sinkCfg;
    }
}
