export is_show_parseDs=all
export FLINK_HOME=/appdata/zt17606/flink-1.13.6
export dataSourceHost=http://10.192.147.1:12345
export dataSourceToken=f94d3be770bad18b080def5953564a8d
${SEATUNNEL_HOME}/bin/start-seatunnel-flink-connector-v2.sh \
-m yarn-cluster \
--detached \
-yjm 1024m -ytm 2048m \
-ys 2 -p 6 -yqu 'default' \
-yD "taskmanager.memory.managed.size=32mb" \
-yD "taskmanager.memory.network.min=16mb" \
-yD "taskmanager.memory.network.max=32mb" \
-ynm "test-prejob" \
-c config/uatDoris_to_proDoris-mltiple.conf