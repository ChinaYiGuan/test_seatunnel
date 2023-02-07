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

package org.apache.seatunnel.translation.serialization;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.type.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Conversion between {@link SeaTunnelRow} & engine's row.
 *
 * @param <T> engine row
 */
public abstract class RowConverter<T> {

    protected final SeaTunnelDataType<?> dataType;

    private static final Cache<String, SeaTunnelRowType> DYNAMIC_ROW_TYPE_CACHE = CacheBuilder.newBuilder()
            .concurrencyLevel(4) // 并发级别
            .initialCapacity(100)// 初始化容量
            .expireAfterWrite(5, TimeUnit.DAYS)//失效时间
            .maximumSize(10000)//最大容量
            .build();

    protected Function<String, SeaTunnelDataType<?>> dynamicRowTypeFunction;

    public RowConverter(SeaTunnelDataType<?> dataType) {
        this.dataType = dataType;
    }

    public RowConverter(SeaTunnelDataType<?> dataType, Function<String, SeaTunnelDataType<?>> dynamicRowTypeFunction) {
        this.dataType = dataType;
        this.dynamicRowTypeFunction = dynamicRowTypeFunction;
    }

    protected SeaTunnelRowType getDynamicRowType(String identifier) {
        SeaTunnelRowType dynamicRowType = DYNAMIC_ROW_TYPE_CACHE.getIfPresent(identifier);
        if (Objects.nonNull(dynamicRowType)) {
            return dynamicRowType;
        } else {
            dynamicRowType = (SeaTunnelRowType) dynamicRowTypeFunction.apply(identifier);
        }
        return dynamicRowType;
    }


    public void validate(SeaTunnelRow seaTunnelRow) throws IOException {
        if (!(dataType instanceof SeaTunnelRowType)) {
            throw new UnsupportedOperationException(String.format("The data type don't support validation: %s. ", dataType.getClass().getSimpleName()));
        }
        if (StringUtils.isNotBlank(seaTunnelRow.getIdentifier())) {
            return;
        }
        SeaTunnelDataType<?>[] fieldTypes = ((SeaTunnelRowType) dataType).getFieldTypes();
        List<String> errors = new ArrayList<>();
        Object field;
        SeaTunnelDataType<?> fieldType;
        for (int i = 0; i < fieldTypes.length; i++) {
            field = seaTunnelRow.getField(i);
            fieldType = fieldTypes[i];
            if (!validate(field, fieldType)) {
                errors.add(String.format("The SQL type '%s' don't support '%s', the class of the expected data type is '%s'.",
                        fieldType.getSqlType(), field.getClass(), fieldType.getTypeClass()));
            }
        }
        if (errors.size() > 0) {
            throw new UnsupportedOperationException(String.join(",", errors));
        }
    }

    protected boolean validate(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null || dataType.getSqlType() == SqlType.NULL) {
            return true;
        }
        SqlType sqlType = dataType.getSqlType();
        switch (sqlType) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case DECIMAL:
            case BYTES:
            case ARRAY:
                return dataType.getTypeClass() == field.getClass();
            case MAP:
                if (!(field instanceof Map)) {
                    return false;
                }
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                Map<?, ?> mapField = (Map<?, ?>) field;
                if (mapField.size() == 0) {
                    return true;
                } else {
                    Map.Entry<?, ?> entry = mapField.entrySet().stream().findFirst().get();
                    return validate(entry.getKey(), mapType.getKeyType())
                            && validate(entry.getValue(), mapType.getValueType());
                }
            case ROW:
                if (!(field instanceof SeaTunnelRow)) {
                    return false;
                }
                SeaTunnelDataType<?>[] fieldTypes = ((SeaTunnelRowType) dataType).getFieldTypes();
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) field;
                for (int i = 0; i < fieldTypes.length; i++) {
                    if (!validate(seaTunnelRow.getField(i), fieldTypes[i])) {
                        return false;
                    }
                }
                return true;
            default:
                return false;
        }
    }

    /**
     * Convert {@link SeaTunnelRow} to engine's row.
     *
     * @throws IOException Thrown, if the conversion fails.
     */
    public abstract T convert(SeaTunnelRow seaTunnelRow) throws IOException;

    /**
     * Convert engine's row to {@link SeaTunnelRow}.
     *
     * @throws IOException Thrown, if the conversion fails.
     */
    public abstract SeaTunnelRow reconvert(T engineRow) throws IOException;
}
