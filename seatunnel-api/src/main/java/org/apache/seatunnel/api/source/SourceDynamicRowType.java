package org.apache.seatunnel.api.source;

import org.apache.seatunnel.api.common.SeaTunnelDynamicRowType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.Serializable;

public interface SourceDynamicRowType<T> extends SeaTunnelDynamicRowType<T> {

    String DYNAMIC_ROW_KEY = "__seatunnel_dynamic_row_type_key";//UUID.randomUUID() + "_" + Instant.now().toEpochMilli();
    BasicType<String> DYNAMIC_ROW_TYPE = BasicType.STRING_TYPE;
    SeaTunnelRowType DYNAMIC_TSF_ROW_TYPE = new SeaTunnelRowType(new String[]{DYNAMIC_ROW_KEY}, new SeaTunnelDataType[]{DYNAMIC_ROW_TYPE});

    default boolean isMultiple() {
        return false;
    }

    default <T, SplitT extends SourceSplit, StateT extends Serializable> SeaTunnelDataType<?> getDynamicProducedType(SeaTunnelSource<T, SplitT, StateT> source) {
        return source.isMultiple() ? DYNAMIC_TSF_ROW_TYPE : source.getProducedType();
    }


}
