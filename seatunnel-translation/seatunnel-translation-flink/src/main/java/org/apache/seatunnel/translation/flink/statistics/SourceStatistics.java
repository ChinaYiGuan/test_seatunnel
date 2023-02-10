package org.apache.seatunnel.translation.flink.statistics;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

@Slf4j
public class SourceStatistics implements BaseStatistics {
    private int totalCnt = 0;
    private Config sourceCfg;
    private SeaTunnelSource<SeaTunnelRow, ?, ?> source;

    public SourceStatistics(Config sourceCfg, SeaTunnelSource<SeaTunnelRow, ?, ?> source) {
        this.sourceCfg = sourceCfg;
        this.source = source;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() {
        log.info("【{}】The statistical value is:{} -> pluginName:{}", getStatisticsId(), totalCnt, source.getPluginName());
    }

    @Override
    public void statistics(Row row) {
        totalCnt++;
    }

    @Override
    public SeaTunnelSource<SeaTunnelRow, ?, ?> getPlugin() {
        return source;
    }

    @Override
    public Config getConfig() {
        return sourceCfg;
    }
}
