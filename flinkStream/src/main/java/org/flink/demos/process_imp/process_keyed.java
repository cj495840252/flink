package org.flink.demos.process_imp;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class process_keyed {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");
        env.setParallelism(1);

        DataStreamSource<String> source1 = env.socketTextStream("node1", 9998);
        // 1.keyby分组
        KeyedStream<Tuple2<String, String>, String> keyBy = source1.map(s -> {
            String[] split = s.split("\\s+");
            return Tuple2.of(split[0], split[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {}).keyBy(key -> key.f0);


        SingleOutputStreamOperator<Tuple2<Integer, String>> process =
                keyBy.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>() {
                    //第一个泛型是key的类型，第二个是这条数据的类型，第三个是返回值
                    private int count;
            @Override
            public void processElement(Tuple2<String, String> t2, KeyedProcessFunction<String, Tuple2<String, String>,
                    Tuple2<Integer, String>>.Context context, Collector<Tuple2<Integer, String>> collector) throws Exception {
                // 这里的数据并没有分组，还是一条一条来的
                count++;
                collector.collect(Tuple2.of(Integer.parseInt(t2.f0), String.valueOf(count)));
            }
        });

        process.print();

        env.execute();
    }
}
