package org.apache.seatunnel.api.common;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

public interface DynamicRowType<T> {

    SeaTunnelDataType<T> getDynamicRowType(String identifier);

}
