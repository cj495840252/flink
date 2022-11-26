package org.flink.demos.worldcount;

import com.alibaba.fastjson.JSON;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.SerializableSupplier;
import org.flink.demos.worldcount.databean.ResultBean;
import org.flink.demos.worldcount.databean.UserInfo;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Collections;

public class demo1 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8099);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        DataStreamSource<UserInfo> source1 = env.addSource(new SourceFunction<UserInfo>() {
            private String[] events = {"click", "download", "upload", "view", "browser"};
            volatile boolean flag = true;

            @Override
            public void run(SourceContext<UserInfo> sourceContext) throws Exception {
                while (flag){
                    UserInfo userInfo = new UserInfo();
                    userInfo.setId(RandomUtils.nextInt(0, 10));
                    userInfo.setEventId(this.events[RandomUtils.nextInt(0, this.events.length)]);
                    userInfo.setNum(RandomUtils.nextInt(0, 100));
                    sourceContext.collect(userInfo);
                    Thread.sleep(RandomUtils.nextLong(500,1500));
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        DataStreamSource<String> source2 = env.socketTextStream("node1", 9999);
        MapStateDescriptor<String, ArrayList<String>> descriptor = new MapStateDescriptor<>("table1",
                TypeInformation.of(String.class), TypeInformation.of(new TypeHint<ArrayList<String>>() {
        }));
        BroadcastStream<String> table1 = source2.broadcast(descriptor);
        SingleOutputStreamOperator<String> restult = source1.connect(table1).process(new BroadcastProcessFunction<UserInfo, String, String>() {
            @Override
            public void processElement(UserInfo userInfo, BroadcastProcessFunction<UserInfo, String, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                ReadOnlyBroadcastState<String, ArrayList<String>> state = readOnlyContext.getBroadcastState(descriptor);
                ArrayList<String> list = state.get(String.valueOf(userInfo.getId()));
                StringBuilder builder = new StringBuilder();
                if (list==null) {
                    builder.append(userInfo.getId()).append(" ").
                            append(userInfo.getEventId()).append(" ")
                            .append(userInfo.getNum()).append(" ")
                            .append("null");
                    collector.collect(builder.toString());
                }
                else {
                    builder.append(userInfo.getId()).append(" ").
                            append(userInfo.getEventId()).append(" ")
                            .append(userInfo.getNum()).append(" ")
                            .append(list.get(1));
                    String s = builder.toString();
                    // 关联上且能被7整除的侧流输出，否则主流
                    if (userInfo.getId() % 3 == 0)
                        readOnlyContext.output(new OutputTag<>("关联上", TypeInformation.of(String.class)), s);
                    else collector.collect(s);
                }

            }

            @Override
            public void processBroadcastElement(String s, BroadcastProcessFunction<UserInfo, String, String>.Context context, Collector<String> collector) throws Exception {
                String[] split = s.split("\\s+");
                BroadcastState<String, ArrayList<String>> broadcastState = context.getBroadcastState(descriptor);
                ArrayList<String> list = new ArrayList<>();
                Collections.addAll(list, split);
                broadcastState.put(split[0], list);
            }
        });


        SingleOutputStreamOperator<Tuple4<String, String, Integer, String>> maxBy = restult.map(s -> {
            String[] split = s.split("\\s+");
            return Tuple4.of(split[0], split[1], Integer.parseInt(split[2]), split[3]);
        }).returns(new TypeHint<Tuple4<String, String, Integer, String>>() {}).keyBy(arr -> arr.f3).maxBy(2);

        maxBy.print("max by ==> ");
        restult.getSideOutput(new OutputTag<>("关联上",TypeInformation.of(String.class))).print("侧流输出 ==> ");

        //关联上的写出到parquet文件
        ParquetWriterFactory<ResultBean> factory = ParquetAvroWriters.forReflectRecord(ResultBean.class);
        FileSink<ResultBean> fileSink = FileSink.forBulkFormat(new Path("F:\\JavaProject\\flink-test\\mode1\\src\\outfile\\data"), factory)
                .withBucketAssigner(new DateTimeBucketAssigner<ResultBean>())
                .withBucketCheckInterval(5)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                        .withPartSuffix(".parquet")
                        .withPartPrefix("侧流输出")
                        .build()
                )
                .build();
        SingleOutputStreamOperator<ResultBean> returns = restult.getSideOutput(new OutputTag<>("关联上", TypeInformation.of(String.class)))
                .map(str -> {
                    String[] split = str.split("\\s+");
                    return new ResultBean(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]), split[3]);
                }).returns(ResultBean.class);
        returns.sinkTo(fileSink);


        // 主流写入mysql
        String sql = "";
        SinkFunction<ResultBean> sink = JdbcSink.exactlyOnceSink(sql, new JdbcStatementBuilder<ResultBean>() {
                    @Override
                    public void accept(PreparedStatement sql, ResultBean resultBean) throws SQLException {
                        sql.setString(4, resultBean.getS());
                        sql.setString(2, resultBean.getEventId());
                        sql.setLong(3, resultBean.getNum());
                        sql.setLong(1, resultBean.getId());
                    }
                }, JdbcExecutionOptions.builder().withMaxRetries(2).withBatchSize(5).build(),
                JdbcExactlyOnceOptions.builder().withTransactionPerConnection(true).build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUser("root");
                        xaDataSource.setUrl("jdbc:mysql://127.0.0.1:3306/test?serverTimezone=Asia/Shanghai&useSSL=false");
                        xaDataSource.setPassword("root");
                        return xaDataSource;
                    }
                });


        returns.addSink(sink);
        env.execute();
    }
}
