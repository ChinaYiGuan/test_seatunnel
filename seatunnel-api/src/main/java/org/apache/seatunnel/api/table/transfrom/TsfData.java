package org.apache.seatunnel.api.table.transfrom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.dynamic.RowIdentifier;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TsfData {

    private RowIdentifier rowIdentifier;
    private List<Data> data;

    @lombok.Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Data {
        private String name;
        private SeaTunnelDataType<?> type;
        private Object value;
    }


}
