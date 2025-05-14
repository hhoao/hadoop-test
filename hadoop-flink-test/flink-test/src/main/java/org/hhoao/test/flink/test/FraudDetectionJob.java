package org.hhoao.test.flink.test;

import java.time.Duration;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        DataStream<Transaction> transactions =
                env.addSource(new TransactionSource()).name("transactions");
        DataStream<Alert> alerts =
                transactions
                        .keyBy(Transaction::getAccountId)
                        .process(new FraudDetector())
                        .setParallelism(8)
                        .name("fraud-detector");

        alerts.addSink(new AlertSink()).setParallelism(8).name("send-alerts");

        env.execute("Fraud Detection");
    }
}
