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
ssh-keygen -t rsa -f /root/.ssh/id_rsa  -N ''
sleep 1
hosts=(node1)
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

echo "=== CONFIG HADOOP START ==="
sleep 1
echo "export JAVA_HOME=${JAVA_HOME}" > /opt/hadoop-3.3.5/etc/hadoop/hadoop-env.sh
  "${HADOOP_HOME}"/bin/hdfs namenode -format
  start-dfs.sh
echo "=== CONFIG HADOOP END==="
