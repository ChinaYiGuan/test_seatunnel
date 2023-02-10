/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.translation.flink.serialization;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.tsf.TsfData;
import org.apache.seatunnel.common.tsf.TsfMeta;
import org.apache.seatunnel.translation.serialization.RowConverter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class FlinkRowConverter extends RowConverter<Row> {


    public FlinkRowConverter(SeaTunnelDataType<?> dataType) {
        super(dataType);
    }

    public FlinkRowConverter(SeaTunnelDataType<?> dataType, Function<String, SeaTunnelDataType<?>> dynamicRowTypeFunction) {
        super(dataType, dynamicRowTypeFunction);
    }

    @Override
    public Row convert(SeaTunnelRow seaTunnelRow) throws IOException {
        validate(seaTunnelRow);
        return (Row) convert(seaTunnelRow, dataType);
    }


    private Object convert(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        SqlType sqlType = dataType.getSqlType();
        switch (sqlType) {
            case ROW:
                return dynamicRowConvert((SeaTunnelRow) field, (SeaTunnelRowType) dataType);
                /*
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) field;
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                int arity = rowType.getTotalFields();
                Row engineRow = new Row(arity);
                for (int i = 0; i < arity; i++) {
                    engineRow.setField(i, convert(seaTunnelRow.getField(i), rowType.getFieldType(i)));
                }
                engineRow.setKind(RowKind.fromByteValue(seaTunnelRow.getRowKind().toByteValue()));

                final String identifier = seaTunnelRow.getIdentifier();
                if (StringUtils.isNotBlank(identifier)) {
                    Row firstRow = Row.of(identifier);
                    return Row.join(firstRow, engineRow);
                }
                return engineRow;
                 */
            case MAP:
                return convertMap((Map<?, ?>) field, (MapType<?, ?>) dataType, this::convert);
            default:
                return field;
        }
    }

    private static Object convertMap(Map<?, ?> mapData, MapType<?, ?> mapType, BiFunction<Object, SeaTunnelDataType<?>, Object> convertFunction) {
        if (mapData == null || mapData.size() == 0) {
            return mapData;
        }
        switch (mapType.getValueType().getSqlType()) {
            case MAP:
            case ROW:
                Map<Object, Object> newMap = new HashMap<>(mapData.size());
                mapData.forEach((key, value) -> {
                    SeaTunnelDataType<?> valueType = mapType.getValueType();
                    newMap.put(key, convertFunction.apply(value, valueType));
                });
                return newMap;
            default:
                return mapData;
        }
    }

    @Override
    public SeaTunnelRow reconvert(Row engineRow) throws IOException {
        return (SeaTunnelRow) reconvert(engineRow, dataType);
    }


    private Object reconvert(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        SqlType sqlType = dataType.getSqlType();
        switch (sqlType) {
            case ROW:
                return dynamicRowReconvert((Row) field, (SeaTunnelRowType) dataType);
/*

                Row engineRow = (Row) field;
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                int arity = rowType.getTotalFields();
                SeaTunnelRow seaTunnelRow = new SeaTunnelRow(arity);
                for (int i = 0; i < arity; i++) {
                    seaTunnelRow.setField(i, reconvert(engineRow.getField(i), rowType.getFieldType(i)));
                }
                seaTunnelRow.setRowKind(org.apache.seatunnel.api.table.type.RowKind.fromByteValue(engineRow.getKind().toByteValue()));
                return seaTunnelRow;
 */
            case MAP:
                return convertMap((Map<?, ?>) field, (MapType<?, ?>) dataType, this::reconvert);
            default:
                return field;
        }
    }

    private Row dynamicRowConvert(SeaTunnelRow seaTunnelRow, SeaTunnelRowType rowType) {
        final String identifier = seaTunnelRow.getIdentifier();
        boolean isDynamic = false;
        if (StringUtils.isNotBlank(identifier) && Objects.nonNull(dynamicRowTypeFunction) && Objects.nonNull(dynamicRowTypeFunction.apply(identifier)) &&
                Arrays.equals(rowType.getFieldNames(), SeaTunnelRowType.DYNAMIC_TSF_ROW_TYPE.getFieldNames()) && Arrays.equals(rowType.getFieldTypes(), SeaTunnelRowType.DYNAMIC_TSF_ROW_TYPE.getFieldTypes())
        ) {
            isDynamic = true;
        }

        final int arity = rowType.getTotalFields();
        Row engineRow;
        if (isDynamic) {
            SeaTunnelRowType tsfRowType = getDynamicRowType(identifier);
            if (Objects.isNull(tsfRowType)) {
                log.error(identifier + " -> The DynamicRowType subclass must implement the getDynamicRowType method!");
                throw new RuntimeException(identifier + " -> The DynamicRowType subclass must implement the getDynamicRowType method!");
            }
            int dynamicArity = tsfRowType.getTotalFields();
            engineRow = new Row(dynamicArity);
            for (int i = 0; i < dynamicArity; i++) {
                engineRow.setField(i, convert(seaTunnelRow.getField(i), tsfRowType.getFieldType(i)));
            }
            final List<TsfData> dataList = new ArrayList<>();
            for (int i = 0; i < dynamicArity; i++) {
                dataList.add(new TsfData(i + "", tsfRowType.getFieldType(i).getTypeClass().getSimpleName(), engineRow.getField(i)));
            }

            final TsfMeta cvtMeta = new TsfMeta();
            cvtMeta.setIdentifier(identifier);
            cvtMeta.setTime(LocalDateTime.now());
            cvtMeta.setNames(dataList.stream().map(TsfData::getName).collect(Collectors.toList()));
            cvtMeta.setTypes(dataList.stream().map(TsfData::getType).collect(Collectors.toList()));

            engineRow = Row.of(
                    JSON.toJSONString(dataList, JSONWriter.Feature.WriteNulls),
                    JSON.toJSONString(cvtMeta, JSONWriter.Feature.WriteNulls)
            );
        } else {
            engineRow = new Row(arity);
            for (int i = 0; i < arity; i++) {
                engineRow.setField(i, convert(seaTunnelRow.getField(i), rowType.getFieldType(i)));
            }
        }

        engineRow.setKind(RowKind.fromByteValue(seaTunnelRow.getRowKind().toByteValue()));

        return engineRow;
    }

    private SeaTunnelRow dynamicRowReconvert(Row engineRow, SeaTunnelRowType engineRowDataType) {
        boolean isDynamic = false;
        List<TsfData> tsfDataList = null;
        TsfMeta tsfMeta = null;
        if (Objects.nonNull(dynamicRowTypeFunction) && engineRow.getArity() == 2 &&
                Arrays.equals(engineRowDataType.getFieldNames(), SeaTunnelRowType.DYNAMIC_TSF_ROW_TYPE.getFieldNames()) && Arrays.equals(engineRowDataType.getFieldTypes(), SeaTunnelRowType.DYNAMIC_TSF_ROW_TYPE.getFieldTypes())) {
            Object tsfDataJsonList = engineRow.getField(0);
            Object tsfMetaJsonObj = engineRow.getField(1);
            if (Objects.nonNull(tsfDataJsonList) && tsfDataJsonList instanceof String && Objects.nonNull(tsfMetaJsonObj) && tsfMetaJsonObj instanceof String) {
                if (JSON.isValidArray(tsfDataJsonList.toString()) && JSON.isValidObject(tsfMetaJsonObj.toString())) {
                    tsfMeta = JSON.parseObject(tsfMetaJsonObj.toString(), TsfMeta.class);
                    if (Objects.nonNull(tsfMeta) && StringUtils.isNotBlank(tsfMeta.getIdentifier())) {
                        tsfDataList = JSON.parseArray((String) engineRow.getField(0), TsfData.class);
                        isDynamic = true;
                    }
                }
            }
        }
        int arity = engineRowDataType.getTotalFields();
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(arity);
        if (isDynamic) {
            String identifier = tsfMeta.getIdentifier();
            SeaTunnelRowType tsfRowType = getDynamicRowType(identifier);
            if (Objects.isNull(tsfRowType)) {
                log.error(identifier + " -> The DynamicRowType subclass must implement the getDynamicRowType method!");
                throw new RuntimeException(identifier + " -> The DynamicRowType subclass must implement the getDynamicRowType method!");
            }
            int dynamicArity = tsfRowType.getTotalFields();
            seaTunnelRow = new SeaTunnelRow(dynamicArity);
            seaTunnelRow.setIdentifier(identifier);
            for (int i = 0; i < dynamicArity; i++) {
                if (tsfDataList.size() > i) {
                    TsfData cvtData = tsfDataList.get(i);
                    Object value = cvtData.getValue();
                    seaTunnelRow.setField(i, reconvert(value, tsfRowType.getFieldType(i)));
                }
            }
        } else {
            for (int i = 0; i < arity; i++) {
                seaTunnelRow.setField(i, reconvert(engineRow.getField(i), engineRowDataType.getFieldType(i)));
            }
        }
        seaTunnelRow.setRowKind(org.apache.seatunnel.api.table.type.RowKind.fromByteValue(engineRow.getKind().toByteValue()));
        return seaTunnelRow;
    }
}
