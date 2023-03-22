package org.apache.seatunnel.common.utils;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.Session;
import cn.hutool.db.ds.DSFactory;
import cn.hutool.db.ds.pooled.PooledDSFactory;
import cn.hutool.db.sql.SqlLog;
import cn.hutool.log.level.Level;
import cn.hutool.setting.Setting;
import lombok.Builder;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;

/*
## 基本配置信息
# JDBC URL，根据不同的数据库，使用相应的JDBC连接字符串
url = jdbc:mysql://<host>:<port>/<database_name>
# 用户名，此处也可以使用 user 代替
username = 用户名
# 密码，此处也可以使用 pass 代替
password = 密码
# JDBC驱动名，可选（Hutool会自动识别）
driver = com.mysql.jdbc.Driver
 */
public class DbHelper {

    private DataSource ds;

    public DbHelper(ConnInfo connInfo) {
        // 初始化SQL显示
        Map<String, String> defaultProps = MapUtil.<String, String>builder()
                .put(SqlLog.KEY_SHOW_SQL, "true")//showSql
                .put(SqlLog.KEY_FORMAT_SQL, "false")//formatSql
                .put(SqlLog.KEY_SHOW_PARAMS, "true")//showParams
                .put(SqlLog.KEY_SQL_LEVEL, Level.DEBUG.name())//sqlLevel
                .build();
        Setting setting = new Setting();
        setting.putAll(defaultProps);

        String url = connInfo.url;
        String username = connInfo.username;
        String password = connInfo.password;
        String driver = connInfo.driver;
        Map<String, String> props = connInfo.props;
        if (StrUtil.isNotBlank(url)) setting.put("url", url);
        if (StrUtil.isNotBlank(url)) setting.put("username", username);
        if (StrUtil.isNotBlank(url)) setting.put("password", password);
        if (StrUtil.isNotBlank(url)) setting.put("driver", driver);
        if (MapUtil.isNotEmpty(props)) setting.putAll(props);
        DSFactory.setCurrentDSFactory(new PooledDSFactory(setting));
        this.ds = DSFactory.get();
    }


    public List<Map<String, Object>> query(String sql, List<Object> params) {
        return execSql(sql, params, true, false);
    }

    public List<Map<String, Object>> exec(String sql, List<Object> params) {
        return execSql(sql, params, false, false);
    }

    public List<Map<String, Object>> execSql(String sqls, List<Object> params, Boolean isQuery, boolean isTransaction) {
        if (CollUtil.isEmpty(params)) params = Collections.emptyList();
        Session session = Session.create(ds);
        List<Map<String, Object>> result = new ArrayList<>();
        for (String sql : sqls.split(";")) {
            try {
                if (StrUtil.isBlank(sql)) continue;
                if (Objects.nonNull(isQuery) && !isQuery) throw new SQLException();
                session.query(sql, params.toArray())
                        .stream()
                        .map(x -> x.entrySet()
                                .stream()
                                .<Map<String, Object>>collect(LinkedHashMap::new, (x1, x2) -> x1.put(x2.getKey(), x2.getValue()), Map::putAll)
                        )
                        .forEach(result::add);
            } catch (SQLException e) {
                if (Objects.nonNull(isQuery) && !isQuery) {
                    try {
                        if (isTransaction) session.beginTransaction();
                        result.add(MapUtil.
                                <String, Object>builder("result", session.execute(sql, params.toArray()))
                                .build());
                        if (isTransaction) session.commit();
                    } catch (SQLException ex) {
                        if (isTransaction) {
                            try {
                                session.rollback();
                            } catch (SQLException exc) {
                                exc.printStackTrace();
                                throw new RuntimeException(exc);
                            }
                        } else {
                            ex.printStackTrace();
                            throw new RuntimeException(ex);
                        }
                    }
                } else {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            } finally {
                if (Objects.nonNull(session))
                    session.close();
            }
        }
        return result;
    }

    @Builder
    public static class ConnInfo {
        private String url;
        private String username;
        private String password;
        private String driver;
        private Map<String, String> props;
    }


}
