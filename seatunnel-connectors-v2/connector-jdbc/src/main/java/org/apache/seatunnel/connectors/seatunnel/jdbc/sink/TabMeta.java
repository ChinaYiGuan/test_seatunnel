package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.Serializable;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TabMeta implements Serializable {

    private String dbTab;
    private SeaTunnelRowType seaTunnelRowType;
    private List<ColMeta> ColMetas;

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class ColMeta {
        private Integer index;
        private String name;
        private SeaTunnelDataType<?> type;
        private Boolean isKey;
    }

    public boolean isKeyTab() {
        return CollectionUtils.isNotEmpty(ColMetas) && ColMetas.stream().anyMatch(x -> x.isKey);
    }

}
