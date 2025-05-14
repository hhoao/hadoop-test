ssh-keygen -t rsa -f /root/.ssh/id_rsa  -N ''

echo "=== CONFIG USER PASSWORD START ==="
sleep 1
expect -c '
spawn passwd
expect {
	"New" { send "123456\r"; exp_continue }
	"Retype" { send "123456\r" }
}
expect eof;'
echo "=== CONFIG USER PASSWORD END==="

echo "=== CONFIG SSH START ==="
sleep 1
hosts=(node1 node2 node3)
for host in "${hosts[@]}"
do
  expect << EOF
  spawn ssh-copy-id ${host}
  expect {
  	"password" { send "123456\r" }
  }
  expect eof;
EOF
done
echo "=== CONFIG SSH END ==="

echo "=== CONFIG ZOOKEEPER START ==="
sleep 1
mkdir "${ZOOKEEPER_HOME}"/data
echo "${NODE_NAME:4}" > "${ZOOKEEPER_HOME}"/data/myid
"${ZOOKEEPER_HOME}"/bin/zkServer.sh start
echo "=== CONFIG ZOOKEEPER END ==="

echo "=== CONFIG HADOOP START ==="
sleep 1
echo "export JAVA_HOME=${JAVA_HOME}" > /opt/hadoop-3.3.5/etc/hadoop/hadoop-env.sh
"${HADOOP_HOME}"/bin/hdfs --daemon start journalnode
sleep 3
if [[ ${NODE_NAME} == 'node1' ]]
then
  "${HADOOP_HOME}"/bin/hdfs namenode -format
  hdfs --daemon start namenode
fi
sleep 2
if [[ ${NODE_NAME} == 'node2' ]]
then
  hdfs namenode -bootstrapStandby
  hdfs --daemon start namenode
fi
sleep 2
if [[ ${NODE_NAME} == 'node1' ]]
then
  hdfs zkfc -formatZK
  hdfs --daemon start zkfc
fi
sleep 2
hdfs --daemon start datanode
if [[ ${NODE_NAME} == 'node1' || ${NODE_NAME} == 'node2' ]]
then
yarn --daemon start resourcemanager
fi
sleep 2
yarn --daemon start nodemanager
if [[ ${NODE_NAME} == 'node1' ]]
then
mapred --daemon start historyserver
hdfs dfs -chmod -R 777 /
fi


echo "=== CONFIG HADOOP END==="
