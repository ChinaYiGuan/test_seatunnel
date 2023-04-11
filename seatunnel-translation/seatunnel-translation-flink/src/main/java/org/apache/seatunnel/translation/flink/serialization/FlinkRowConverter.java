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
import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.JSONWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.seatunnel.api.source.SourceDynamicRowType;
import org.apache.seatunnel.api.table.transfrom.DataTypeInfo;
import org.apache.seatunnel.api.table.transfrom.TsfData;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.dynamic.RowIdentifier;
import org.apache.seatunnel.translation.serialization.RowConverter;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class FlinkRowConverter extends RowConverter<Row> {


    public FlinkRowConverter(DataTypeInfo dataTypeInfo) {
        super(dataTypeInfo);
    }

    @Override
    public Row convert(SeaTunnelRow seaTunnelRow) throws IOException {
        validate(seaTunnelRow);
        return (Row) convert(seaTunnelRow, dataTypeInfo.getDataType(), dataTypeInfo.isMultiple(), this::getDataType);
    }


    private Object convert(Object field, SeaTunnelDataType<?> dataType, boolean isMultiple, Function<String, SeaTunnelDataType<?>> dataTypeFun) {
        if (field == null) {
            return null;
        }
        SqlType sqlType = dataType.getSqlType();
        switch (sqlType) {
            case ROW:
                try {
                    if (isMultiple) {
                        return multipleRowConvert((SeaTunnelRow) field, dataTypeFun);
                    }
                } catch (Exception e) {
                    log.warn("multiple convert err:{}", e.getMessage());
                }
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) field;
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                int arity = rowType.getTotalFields();
                Row engineRow = new Row(arity);
                for (int i = 0; i < arity; i++) {
                    engineRow.setField(i, convert(seaTunnelRow.getField(i), rowType.getFieldType(i), false, null));
                }
                engineRow.setKind(RowKind.fromByteValue(seaTunnelRow.getRowKind().toByteValue()));
                return engineRow;

            case MAP:
                return convertMap((Map<?, ?>) field, (MapType<?, ?>) dataType, (d, dType) -> convert(d, dType, false, null));
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
        return (SeaTunnelRow) reconvert(engineRow, dataTypeInfo.getDataType(), dataTypeInfo.isMultiple(), this::getDataType);
    }


    private Object reconvert(Object field, SeaTunnelDataType<?> dataType, boolean isMultiple, Function<String, SeaTunnelDataType<?>> dataTypeFun) {
        if (field == null) {
            return null;
        }
        SqlType sqlType = dataType.getSqlType();
        switch (sqlType) {
            case ROW:
                Row engineRow = (Row) field;
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                try {
                    log.info("SeaTunnelRowTypeSeaTunnelRowTypeSeaTunnelRowType "+isMultiple+" "+engineRow +" "+ engineRow.getFieldNames(true)+" "+engineRow.getField(SourceDynamicRowType.DYNAMIC_ROW_KEY));
                    if (isMultiple && engineRow.getArity() == 1 && Objects.requireNonNull(engineRow.getFieldNames(true)).contains(SourceDynamicRowType.DYNAMIC_ROW_KEY) && engineRow.getField(SourceDynamicRowType.DYNAMIC_ROW_KEY) != null) {
                        return multipleRowReconvert((Row) field, dataTypeFun);
                    }
                } catch (Exception e) {
                    log.warn("multiple reconvert err:{}", e.getMessage());
                }
                int arity = rowType.getTotalFields();
                SeaTunnelRow seaTunnelRow = new SeaTunnelRow(arity);
                for (int i = 0; i < arity; i++) {
                    seaTunnelRow.setField(i, reconvert(engineRow.getField(i), rowType.getFieldType(i), false, null));
                }
                seaTunnelRow.setRowKind(org.apache.seatunnel.api.table.type.RowKind.fromByteValue(engineRow.getKind().toByteValue()));
                return seaTunnelRow;

            case MAP:
                return convertMap((Map<?, ?>) field, (MapType<?, ?>) dataType, (d, dType) -> reconvert(d, dType, false, null));
            default:
                return field;
        }
    }

    private Row multipleRowConvert(SeaTunnelRow seaTunnelRow, Function<String, SeaTunnelDataType<?>> dataTypeFun) {
        RowIdentifier rowIdentifier = seaTunnelRow.getRowIdentifier();
        if (rowIdentifier == null)
            throw new RuntimeException("multiple row convert, missing rowIdentifier.");
        String identifier = rowIdentifier.getIdentifier();
        if (identifier == null)
            throw new RuntimeException("multiple row convert, missing identifier.");
        SeaTunnelRowType dynamicDataType = (SeaTunnelRowType) dataTypeFun.apply(identifier);
        if (dynamicDataType == null)
            throw new RuntimeException("multiple row convert, missing dynamicDataType.");
        int arity = dynamicDataType.getTotalFields();
        if (seaTunnelRow.getArity() < arity)
            throw new RuntimeException("multiple row convert, data and type do not correspond.");

        TsfData tsfData = new TsfData();
        tsfData.setRowIdentifier(rowIdentifier);
        tsfData.setData(IntStream.range(0, arity)
                .mapToObj(x -> {
                    String name = dynamicDataType.getTypeClass().getSimpleName();
                    SeaTunnelDataType<?> type = dynamicDataType.getFieldType(x);
                    Object value = seaTunnelRow.getField(x);
                    return new TsfData.Data(name, type, value);
                }).collect(Collectors.toList()));

        Row engineRow = Row.of(JSON.toJSONString(tsfData, JSONWriter.Feature.WriteNulls));
        engineRow.setKind(RowKind.fromByteValue(seaTunnelRow.getRowKind().toByteValue()));
        return engineRow;
    }

    private SeaTunnelRow multipleRowReconvert(Row engineRow, Function<String, SeaTunnelDataType<?>> dataTypeFun) {
        Object tsfDataJson = engineRow.getField(0);
        log.info("f00000 "+ tsfDataJson);
        if (tsfDataJson == null)
            throw new RuntimeException("multiple row reconvert, conversion data is empty.");
        TsfData tsfData = JSON.parseObject(tsfDataJson.toString(), TsfData.class, JSONReader.Feature.SupportClassForName);
        if (tsfData == null)
            throw new RuntimeException("multiple row reconvert, parsing data is empty.");
        RowIdentifier rowIdentifier = tsfData.getRowIdentifier();
        if (rowIdentifier == null)
            throw new RuntimeException("multiple row reconvert, missing rowIdentifier.");
        String identifier = rowIdentifier.getIdentifier();
        if (identifier == null)
            throw new RuntimeException("multiple row reconvert, missing identifier.");
        List<TsfData.Data> dataList = tsfData.getData();
        SeaTunnelRowType dynamicDataType = (SeaTunnelRowType) dataTypeFun.apply(identifier);
        log.info("dataTypeFundataTypeFundataTypeFundataTypeFun"+dataTypeFun+" "+dynamicDataType);
        if (dynamicDataType == null)
            throw new RuntimeException("multiple row reconvert, dynamicDataType is empty.");
        int arity = dynamicDataType.getTotalFields();
        if (dataList.size() < arity)
            throw new RuntimeException("multiple row reconvert, data and type do not correspond.");
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(arity);
        ArrayList<String> names = new ArrayList<>();
        ArrayList<SeaTunnelDataType<?>> types = new ArrayList<>();
        IntStream.range(0, dataList.size()).forEach(x -> {
            TsfData.Data data = dataList.get(x);
            SeaTunnelDataType<?> fieldType = dynamicDataType.getFieldType(x);
            seaTunnelRow.setField(x, reconvert(data.getValue(), fieldType, false, null));
            names.add(data.getName());
            types.add(data.getType());
        });

        rowIdentifier.getMetaMap().put("names", names);
        rowIdentifier.getMetaMap().put("types", types);
        seaTunnelRow.setRowIdentifier(rowIdentifier);
        seaTunnelRow.setRowKind(org.apache.seatunnel.api.table.type.RowKind.fromByteValue(engineRow.getKind().toByteValue()));

        return seaTunnelRow;
    }
}
