package org.flink.demos.sink_function;

import com.alibaba.fastjson.JSON;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.flink.demos.source_function.init.EventLog;
import org.flink.demos.source_function.init.MySourceFunction;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyJDBCSink {
    public static void main(String[] args) throws Exception {
        String path = "file:///F:/JavaProject/flink-test/mode1/src/outfile/";
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(path+"checkpoint");

        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());


        String jdbc_url="jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF8&useSSL=false&serverTimezone=UTC&transformedBitIsBoolean=true&allowMultiQueries=true";
        String sql ="insert into eventlog values (?,?,?,?,?) on duplicate key update sessionId=?,eventId=?,`ts`=?,eventInfo=?";
        JdbcStatementBuilder<EventLog> jdbcStatementBuilder = new JdbcStatementBuilder<EventLog>() {
            @Override
            public void accept(PreparedStatement sql, EventLog eventLog) throws SQLException {
                // 第一个参数代表第一个sql参数，第二个代表传进来的数据
                sql.setLong(1, eventLog.getGuid());
                sql.setString(2, eventLog.getSessionId());
                sql.setString(3, eventLog.getEventId());
                sql.setLong(4, eventLog.getTimeStamp());
                sql.setString(5, JSON.toJSONString(eventLog.getEventInfo()));
                sql.setString(6, eventLog.getSessionId());
                sql.setString(7, eventLog.getEventId());
                sql.setLong(8, eventLog.getTimeStamp());
                sql.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
            }
        };



        // 不保证精确一次：exactly-once:eos
        SinkFunction<EventLog> jdbcSink1 = JdbcSink.sink(
                sql,jdbcStatementBuilder
                , JdbcExecutionOptions.builder().withBatchSize(5).withMaxRetries(2).build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withUsername("root")
                        .withPassword("root")
                        // mysql6以上要加时区，不然会出错Asia/Shanghai == GMT%2B（即GMT+8的url编码结果）
                        //"jdbc:mysql://localhost:3306/test?serverTimezone=Asia/Shanghai&useSSL=false"
                        .withUrl(jdbc_url)
                        //.withConnectionCheckTimeoutSeconds(5)
                        //.withDriverName("com.mysql.connector.Driver")
                        .build()
        );

        // 保证精确一次：exactly-once
        SinkFunction<EventLog> jdbcSink2 = JdbcSink.exactlyOnceSink(sql,
                jdbcStatementBuilder,
                JdbcExecutionOptions.builder().withBatchSize(5).withMaxRetries(2).build(),
                JdbcExactlyOnceOptions.builder()
                        //mysql只支持一个连接只能有一个事务，这里要设置true
                        .withTransactionPerConnection(true)
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUser("root");
                        xaDataSource.setUrl("jdbc:mysql://127.0.0.1:3306/test?serverTimezone=Asia/Shanghai&useSSL=false");
                        xaDataSource.setPassword("root");
                        return xaDataSource;
                    }
                }
        );
        source.addSink(jdbcSink2);

        env.execute();
    }
}