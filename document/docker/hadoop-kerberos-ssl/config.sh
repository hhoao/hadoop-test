#!/bin/bash

useradd nn
useradd hadoop
useradd hdfs
useradd yarn
useradd mapred
groupadd supergroup
usermod -a -G supergroup hadoop

node_hosts=(node1 node2 node3)

if [[ ${NODE_NAME:0:4} == "node" ]]; then
  cp /config/zoo.cfg /opt/zookeeper-3.8.1/conf/zoo.cfg
  cp /config/jass.conf /opt/zookeeper-3.8.1/conf/jass.conf
  cp /config/java.env /opt/zookeeper-3.8.1/conf/java.env
  cp /config/core-site.xml /opt/hadoop-3.3.5/etc/hadoop/core-site.xml
  cp /config/hdfs-site.xml /opt/hadoop-3.3.5/etc/hadoop/hdfs-site.xml
  cp /config/yarn-site.xml /opt/hadoop-3.3.5/etc/hadoop/yarn-site.xml
  cp /config/mapred-site.xml /opt/hadoop-3.3.5/etc/hadoop/mapred-site.xml
  cp /config/ssl-client.xml /opt/hadoop-3.3.5/etc/hadoop/ssl-client.xml
  cp /config/ssl-server.xml /opt/hadoop-3.3.5/etc/hadoop/ssl-server.xml
  cp /config/container-executor.cfg /opt/hadoop-3.3.5/etc/hadoop/container-executor.cfg
fi
cp /config/kdc.conf /etc/krb5kdc/kdc.conf
cp /config/krb5.conf /etc/krb5.conf
cp /config/kadm5.acl /etc/krb5kdc/kadm5.acl

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

echo "=== CONFIG CA AND KERBEROS START ==="
if [[ -d /opt/keytab_store ]]; then
  rm -rf /opt/keytab_store
fi
if [[ -d /opt/ssl_store ]]; then
  rm -rf /opt/ssl_store/*
fi
mkdir -p /opt/keytab_store /opt/ssl_store
sleep 1
if [[ ${NODE_NAME} == "kdc" ]]; then
  cd /opt/keytab_store || echo "cd /opt/keytab_store failed"
  echo -e "123\n123" | kdb5_util create -s -r TEST.COM
  kadmin.local addprinc -randkey HTTP/node1@TEST.COM
  kadmin.local addprinc -randkey HTTP/node2@TEST.COM
  kadmin.local addprinc -randkey HTTP/node3@TEST.COM

  kadmin.local addprinc -randkey hadoop/node1@TEST.COM
  kadmin.local addprinc -randkey hadoop/node2@TEST.COM
  kadmin.local addprinc -randkey hadoop/node3@TEST.COM

  kadmin.local addprinc -randkey nn/node1@TEST.COM
  kadmin.local addprinc -randkey nn/node2@TEST.COM

  kadmin.local addprinc -randkey dn/node1@TEST.COM
  kadmin.local addprinc -randkey dn/node2@TEST.COM
  kadmin.local addprinc -randkey dn/node3@TEST.COM

  kadmin.local addprinc -randkey jn/node1@TEST.COM
  kadmin.local addprinc -randkey jn/node2@TEST.COM
  kadmin.local addprinc -randkey jn/node3@TEST.COM

  kadmin.local addprinc -randkey rm/node1@TEST.COM
  kadmin.local addprinc -randkey rm/node2@TEST.COM

  kadmin.local addprinc -randkey nm/node1@TEST.COM
  kadmin.local addprinc -randkey nm/node2@TEST.COM
  kadmin.local addprinc -randkey nm/node3@TEST.COM

  kadmin.local addprinc -randkey zk/node1@TEST.COM
  kadmin.local addprinc -randkey zk/node2@TEST.COM
  kadmin.local addprinc -randkey zk/node3@TEST.COM

  kadmin.local addprinc -randkey jhs/node1@TEST.COM

  kadmin.local ktadd -k /opt/keytab_store/http.service.keytab HTTP/node1@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/http.service.keytab HTTP/node2@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/http.service.keytab HTTP/node3@TEST.COM

  kadmin.local ktadd -k /opt/keytab_store/nn.service.keytab nn/node1@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/nn.service.keytab nn/node2@TEST.COM

  kadmin.local ktadd -k /opt/keytab_store/dn.service.keytab dn/node1@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/dn.service.keytab dn/node2@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/dn.service.keytab dn/node3@TEST.COM

  kadmin.local ktadd -k /opt/keytab_store/jn.service.keytab jn/node1@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/jn.service.keytab jn/node2@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/jn.service.keytab jn/node3@TEST.COM

  kadmin.local ktadd -k /opt/keytab_store/rm.service.keytab rm/node1@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/rm.service.keytab rm/node2@TEST.COM

  kadmin.local ktadd -k /opt/keytab_store/nm.service.keytab nm/node1@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/nm.service.keytab nm/node2@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/nm.service.keytab nm/node3@TEST.COM

  kadmin.local ktadd -k /opt/keytab_store/zk.service.keytab zk/node1@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/zk.service.keytab zk/node2@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/zk.service.keytab zk/node3@TEST.COM

  kadmin.local ktadd -k /opt/keytab_store/jhs.service.keytab jhs/node1@TEST.COM

  kadmin.local ktadd -k /opt/keytab_store/hadoop.keytab hadoop/node1@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/hadoop.keytab hadoop/node2@TEST.COM
  kadmin.local ktadd -k /opt/keytab_store/hadoop.keytab hadoop/node3@TEST.COM

  service krb5-admin-server start
  service krb5-kdc start

  chmod 777 -R /opt/keytab_store/*

  echo "=== KDC DISTRIBUTE START ==="
  cd /opt/keytab_store || echo "cd /opt/keytab_store false"
  for host in "${node_hosts[@]}"; do
    expect <<EOF
    spawn scp dn.service.keytab  http.service.keytab  jhs.service.keytab  jn.service.keytab  nm.service.keytab  nn.service.keytab  zk.service.keytab rm.service.keytab root@$host:/opt/keytab_store/
    expect {
      "password" { send "123456\r" }
    }
    expect eof;
EOF
  done
  echo "=== KDC DISTRIBUTE END ==="

  echo "=== GENERATE SSL_STORE START ==="
  cd /opt/ssl_store || echo "cd /opt/ssl_store failed"
  # 生成CA私钥
  expect <<EOF
  spawn openssl req -new -x509 -keyout ca_private.key -out ca_cert -days 9999 -subj /C=CN/ST=chengdu/L=chengdu/O=bigdata/OU=bigdata/CN=kdc
  expect {
    "Enter PEM pass phrase:" { send "123456\r"; exp_continue; }
  }
EOF
  #生成各个节点的的公私秘钥对
  for host in "${node_hosts[@]}"; do
  expect <<EOF
  spawn keytool -keystore "${host}_keystore" -alias localhost -validity 9999 -genkey -keyalg RSA -keysize 2048 -dname "CN=${host}, OU=bigdata, O=bigdata, L=chengdu, ST=chengdu, C=CN"
  expect {
    "Enter keystore password:" { send "123456\r";  exp_continue; }
    "Re-enter new password:" { send "123456\r"; exp_continue; }
    "password):" { send "\r" }
  }
  expect eof;
EOF
  # 将上述的CA公钥导入节点的信任区truststore
  echo -e "123456\n123456\nyes" | keytool -keystore "${host}_truststore" -alias CARoot -import -file ca_cert
  # 将上述的CA公钥导入本机的keystore中
#  echo -e "123456\nyes" | keytool -keystore "${host}_keystore" -alias CARoot -import -file ca_cert
  # 将本机的公钥证书导出
  echo -e "123456\n" | keytool -certreq -alias localhost -keystore "${host}_keystore" -file "${host}_cert"
  # 用CA私钥，对本机的公钥证书进行签名
  expect <<EOF
  spawn openssl x509 -req -CA ca_cert -CAkey ca_private.key -in "${host}_cert" -out "${host}_cert_signed" -days 9999 -CAcreateserial
  expect "Enter pass phrase for ca_private.key:" { send "123456\r" }
  expect eof;
EOF
  done
  # 将签名后的证书导入的对应节点的Keystore
  for import_host in "${node_hosts[@]}"; do
    echo -e "123456\n" | keytool -keystore "${import_host}_keystore" -alias "${import_host}" -import -file "${import_host}_cert_signed"
    # 在将签名后的证书导入各节点的truststore
    for host in "${node_hosts[@]}"; do
        echo -e "123456\n123456\nyes" | keytool -keystore "${host}_truststore" -alias "${import_host}" -import -file "${import_host}_cert_signed"
        # 在将签名后的证书导入各节点的truststore
    done
  done
  # 将truststore和keystore分发给对应节点
  cd /opt/ssl_store || echo "cd /opt/ssl_store failed"
  for host in "${node_hosts[@]}"; do
    expect <<EOF
    spawn scp ${host}_truststore "${host}_keystore" root@$host:/opt/ssl_store
    expect {
    	"password" { send "123456\r" }
    }
    expect eof;
EOF
  done
  echo "=== GENERATE SSL_STORE END ==="
fi
echo "=== CONFIG CA END ==="

if [[ ${NODE_NAME:0:4} == "node" ]]; then
  echo "=== CONFIG HADOOP START==="
  chown root:hadoop ${HADOOP_HOME}/etc/hadoop/container-executor.cfg
  chmod 6050 ${HADOOP_HOME}/etc/hadoop/container-executor.cfg

  sleep 4
  echo "=== CONFIG ZOOKEEPER START ==="
  sleep 1
  mkdir "${ZOOKEEPER_HOME}"/data
  echo "${NODE_NAME:4}" >"${ZOOKEEPER_HOME}"/data/myid
  "${ZOOKEEPER_HOME}"/bin/zkServer.sh start
  echo "=== CONFIG ZOOKEEPER END ==="

  echo "=== CONFIG HADOOP START ==="
  sleep 1
  echo "export JAVA_HOME=${JAVA_HOME}" >/opt/hadoop-3.3.5/etc/hadoop/hadoop-env.sh
  "${HADOOP_HOME}"/bin/hdfs --daemon start journalnode
  sleep 3
  if [[ ${NODE_NAME} == 'node1' ]]; then
    "${HADOOP_HOME}"/bin/hdfs namenode -format
    hdfs --daemon start namenode
  fi
  sleep 3
  if [[ ${NODE_NAME} == 'node2' ]]; then
    hdfs namenode -bootstrapStandby
    hdfs --daemon start namenode
  fi
  sleep 3
  if [[ ${NODE_NAME} == 'node1' ]]; then
    hdfs zkfc -formatZK
    hdfs --daemon start zkfc
  fi
  sleep 3
  hdfs --daemon start datanode
  if [[ ${NODE_NAME} == 'node1' || ${NODE_NAME} == 'node2' ]]; then
    yarn --daemon start resourcemanager
  fi
  sleep 3

  chown root:hadoop -R "${HADOOP_HOME}"
  chmod 6050 "${HADOOP_HOME}/bin/container-executor"

  yarn --daemon start nodemanager
  if [[ ${NODE_NAME} == 'node1' ]]; then
    kinit -k -t /opt/keytab_store/nn.service.keytab nn/node1
    hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history /tmp/logs
    hdfs dfs -chown jhs:hadoop /tmp/hadoop-yarn/staging/history /tmp/logs
    hdfs dfs -chmod 741 /tmp/hadoop-yarn/staging/history/done /tmp/logs
    mapred --daemon start historyserver
  fi
  echo "=== CONFIG HADOOP END==="
fi

echo "=========================ALL DONE========================="
