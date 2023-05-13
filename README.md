# hadoop、yarn、hdfs、mapreduce、spark、zookeeper、hbase、hive单元测试
- [x] 可以使用Docker本地部署集群进行测试, 支持kerberos部署
- [x] yarn+hdfs+mapreduce单元测试
- [x] yarn+hdfs+spark单元测试
- [x] yarn+hdfs+flink单元测试
- [x] yarn+hdfs+zookeeper+hbase单元测试
- [x] yarn+hdfs+zookeeper+hiveserver2+hivemetastore单元测试

docker 集群需要相应的镜像才可进行部署，镜像构建还需下载相应hadoop文件，不过build目录中都具有相应的下载脚本。

spark单元测试需要上传$ {SPARK_HOME}/jars 到 data/jars 目录，还需要上传 hadoop-common-3.y.z-tests.jar 到目录中
