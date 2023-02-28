package org.apache.seatunnel.common.dynamic;

import com.alibaba.fastjson2.JSON;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class RowIdentifier implements Serializable {

    /**
     * The unique identifier, usually the table name
     */
    private String identifier;
    private String identifierFullName;
    /**
     * meta map
     */
    private Map<String, Object> metaMap;

    private String pluginName;
    private String catalog;
    private String schema;
    private String db;
    private String table;
    private Long tsfTs;
    private Long dataTs;

    public RowIdentifier() {
    }

    private RowIdentifier(Builder builder) {
        this.catalog = builder.catalog;
        this.schema = builder.schema;
        this.db = builder.db;
        this.table = builder.table;

        this.dataTs = builder.dataTs;
        this.tsfTs = builder.tsfTs;
        this.pluginName = builder.pluginName;
        this.identifier = builder.identifier;
        this.identifierFullName = builder.identifierFullName;
        this.metaMap = builder.metaMap;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String pluginName;
        private String catalog;
        private String schema;
        private String db;
        private String table;
        private Long tsfTs;
        private Long dataTs;
        private String identifier;
        private String identifierFullName;
        private Map<String, Object> metaMap = new LinkedHashMap<>();
        private String defaultSplitToken = ".";

        private Builder() {
        }

        public Builder splitBy(String splitToken) {
            this.defaultSplitToken = splitToken;
            return this;
        }

        public Builder addMeta(String key, Object Value) {
            this.metaMap.put(key, Value);
            return this;
        }

        public Builder pluginName(String pluginName) {
            this.pluginName = pluginName;
            return this;
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder dataTs(Long schema) {
            this.dataTs = dataTs;
            return this;
        }

        public RowIdentifier build(String db, String table) {
            this.db = db;
            this.table = table;
            String identifierTmp = db + defaultSplitToken + table;
            if (StringUtils.isNotBlank(schema))
                identifierTmp = schema + defaultSplitToken + identifierTmp;
            if (StringUtils.isNotBlank(catalog))
                identifierTmp = catalog + defaultSplitToken + identifierTmp;
            return build(identifierTmp);
        }

        public RowIdentifier build(String identifierFullName) {
            this.tsfTs = Instant.now().toEpochMilli();
            this.identifierFullName = identifierFullName;
            if (StringUtils.isNotBlank(identifierFullName)) {
                final String[] splits = StringUtils.split(identifierFullName, defaultSplitToken);
                final int len = splits.length;
                int startIndex = 1;
                if (len >= startIndex) {
                    this.table = splits[len - startIndex++];
                }
                if (len >= startIndex) {
                    this.db = splits[len - startIndex++];
                }
                if (len >= startIndex) {
                    this.schema = splits[len - startIndex++];
                }
                if (len >= startIndex) {
                    this.catalog = splits[len - startIndex++];
                }
            }
            this.identifier = table;
            return new RowIdentifier(this);
        }
    }

    public static void main(String[] args) {
        RowIdentifier build = RowIdentifier.newBuilder()
                .addMeta("aaaa", "dffff")
                .pluginName("pluginName")
                .build("hive.db.tab1");
        System.out.println(JSON.toJSONString(build));
    }
}
