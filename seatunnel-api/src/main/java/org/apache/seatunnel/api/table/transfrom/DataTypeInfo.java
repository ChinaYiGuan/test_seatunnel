package org.apache.seatunnel.api.table.transfrom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.util.function.Function;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataTypeInfo {

    private boolean isMultiple;
    private SeaTunnelDataType<?> dataType;
    private Function<String, SeaTunnelDataType<?>> dataTypeFun;
}
