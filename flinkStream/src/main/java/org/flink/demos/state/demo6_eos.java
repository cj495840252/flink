package org.flink.demos.state;

import com.mysql.cj.jdbc.MysqlXAConnection;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author 陈佳
 * @date 2022-11-13
 * @Description: flink的端到端精确一次容错代码example
 *
 * 1.从kafka读取数据（里面有operator-state状态）
 * 2.处理过程中用到带状态的map算子（里面用keyed-state状态）,逻辑：输入一个字符串，变大写，拼接此前的输入
 * 3.用exactly-oncce的mysql-sink算子输出数据（附带主键的幂等性）
 * 测试用的kafka-topic
 *
 * 测试用的输入数据
 *
 * 测试用的mysql表
 **/

public class demo6_eos {
    public static void main(String[] args) throws Exception {

        System.setProperty("log.file","./webui");
        Configuration conf = new Configuration();
        conf.setString("web.log.path","./webui");


        // conf.setString("execution.savepoint.path","file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint/3f5740a390c03d8d77e61cf7247796d2/chk-109");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);
        // checkpoint容错相关参数
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");
        // task级别故障自动重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.milliseconds(1000)));
        //env.setRestartStrategy(RestartStrategies.noRestart());
        // 状态后端设置，默认：HashMapBackend
        env.setStateBackend(new HashMapStateBackend());

        KafkaSource<String> kafkaBuild = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setTopics("eos")
                .setGroupId("test1")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 允许kafka consumer自动提交消费位移到__consumer_offsets，默认为true
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                // 做checkpoint时，是否自动提交偏移量
                .setProperty("commit.offsets.on.checkpoint", "false")
                .setProperty("partition.discovery.interval.ms", "1000")
                .setClientIdPrefix("Kafka-prefix")

                // 如果没有可用的偏移量，则数据读取策略
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();

        DataStreamSource<String> source = env.fromSource(kafkaBuild, WatermarkStrategy.noWatermarks(), "kafkaSource");
        SingleOutputStreamOperator<String> stream = source.keyBy(k -> "gourp1").map(new RichMapFunction<String, String>() {
            ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("preStr", String.class));
            }

            @Override
            public String map(String s) throws Exception {

                String preStr = valueState.value();
                if (preStr == null) preStr = "";
                valueState.update(preStr+s);

                /*埋一个异常，当接收到x的时候有1/3的该列异常*/
                if ("x".equals(s) && RandomUtils.nextInt(1,4)%3 == 0)
                    throw new Exception("chenjia throw a exception......");
                return preStr + s.toUpperCase();
            }
        });

        // 构造jdbc输出的sink
        SinkFunction<String> sinkFunction = JdbcSink.exactlyOnceSink(
                "insert into eos values (?) on duplicate key update str = ?",
                new JdbcStatementBuilder<String>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, String s) throws SQLException {
                        preparedStatement.setString(1, s);
                        preparedStatement.setString(2, s);
                    }
                }
                ,
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .withMaxRetries(3)
                        .build()
                , JdbcExactlyOnceOptions.builder()
                        //myql每个连接必须只能有一个未完成的事务，这里必须设置成true
                        .withTransactionPerConnection(true)
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        // XADateSource就是jdbc连接，不过它是支持分布式事务的连接，不同数据库他的构造方法也不同
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUrl("jdbc:mysql://192.168.40.42:3306/test?useSSL=true&serverTimezone=GMT%2B8");
                        xaDataSource.setUser("root");
                        xaDataSource.setPassword("root");
                        return xaDataSource;
                    }
                });

        stream.addSink(sinkFunction);


        env.execute();

    }
}
