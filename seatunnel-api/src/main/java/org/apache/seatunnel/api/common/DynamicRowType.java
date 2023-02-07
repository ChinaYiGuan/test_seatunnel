package org.apache.seatunnel.api.common;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.LinkedHashMap;
import java.util.Map;

public interface DynamicRowType<T> {

    SeaTunnelDataType<T> getDynamicRowType(String identifier);

}
