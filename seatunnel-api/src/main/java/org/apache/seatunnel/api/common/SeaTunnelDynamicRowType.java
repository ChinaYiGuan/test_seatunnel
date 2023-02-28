package org.apache.seatunnel.api.common;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

public interface SeaTunnelDynamicRowType<T> {

    default SeaTunnelDataType<T> getDynamicRowType(String identifier){
        return null;
    }

}
