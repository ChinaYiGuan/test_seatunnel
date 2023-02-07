package org.apache.seatunnel.connectors.doris.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

public class DorisTypeConvertUtil {

    public static SeaTunnelDataType<?> convertFromColumn(String typeName) {
        if (StringUtils.isBlank(typeName)) return BasicType.VOID_TYPE;
        String correctTypeName = StringUtils.substringBefore(typeName.toUpperCase().trim(), "(");
        SeaTunnelDataType<?> convertType = BasicType.STRING_TYPE;
        switch (correctTypeName) {
            case "BOOLEAN":
                convertType = BasicType.BOOLEAN_TYPE;
                break;
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "BIGINT":
            case "LARGEINT":
                convertType = BasicType.INT_TYPE;
                break;
            case "FLOAT":
                convertType = BasicType.FLOAT_TYPE;
                break;
            case "DOUBLE":
                convertType = BasicType.DOUBLE_TYPE;
                break;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMALV3":
                String tmp1 = StringUtils.substringBetween(typeName, "(", ")");
                int precision = 64;
                int scale = 8;
                if (StringUtils.isNotBlank(tmp1)) {
                    precision = Integer.parseInt(tmp1.split(",")[0]);
                    scale = Integer.parseInt(tmp1.split(",")[1]);
                }
                convertType = new DecimalType(precision, scale);
                break;
            case "DATE":
            case "DATEV2":
                convertType = LocalTimeType.LOCAL_DATE_TYPE;
                break;
            case "DATETIME":
            case "DATETIMEV2":
            case "DATETIMEV3":
                convertType = LocalTimeType.LOCAL_DATE_TIME_TYPE;
                break;
            case "CHAR":
                convertType = BasicType.STRING_TYPE;
                break;
        }
        return convertType;
    }

}
