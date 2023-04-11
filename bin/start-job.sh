__curr_pwd=$(
  cd $(dirname $0)
  pwd
)
__conf_name="$*"
cd $__curr_pwd
__seatunnel_home=$(
  cd $__curr_pwd/..
  pwd
)

export is_show_parseDs=all

export FLINK_HOME=/appdata/flink-1.13.6
export dataSourceHost=http://10.192.147.1:12345
export dataSourceToken=f94d3be770bad18b080def5953564a8d
export flinkui=http://flink-bi.uat.eminxing.com:81/001
export FLINK_CLUSTER_SESSION_ID=001
export FLINK_ENV_JAVA_OPTS="-Xms128m -Xmx1024m"

unset HADOOP_CLASSPATH

cmd="${__seatunnel_home}/bin/start-seatunnel-flink-connector-v2.sh -d -c ${__seatunnel_home}/${__conf_name}"
echo "$cmd"
sh -c "$cmd"
