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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class process_common {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");
        env.setParallelism(1);

        DataStreamSource<String> source1 = env.socketTextStream("node1", 9999);
        // 使用process实现将数据转换成元组
        SingleOutputStreamOperator<Tuple2<String, String>> process = source1.process(new ProcessFunction<String, Tuple2<String, String>>() {
            @Override
            public void processElement(String s, ProcessFunction<String, Tuple2<String, String>>.Context context, Collector<Tuple2<String, String>> collector) throws Exception {
                /*该方法是必须要实现的
                * s:传入的数据
                * context:上下文对象，里面封装了当前的运行环境
                * collector:收集器，每一个返回的数据由它收集
                * */
                String[] split = s.split("\\s+");


                collector.collect(Tuple2.of(split[0],split[1]));

                //侧流输出
                context.output(new OutputTag<>("侧流1", Types.STRING),s);

            }

            @Override
            public void onTimer(long timestamp, ProcessFunction<String, Tuple2<String, String>>.OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                // 可以使用生命周期方法Open和Close，得到很多上下文信息
                RuntimeContext runtimeContext = getRuntimeContext();
                runtimeContext.getTaskName();//获取task name
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                // 关闭这个process
                super.close();
            }
        });


        DataStreamSource<String> source2 = env.socketTextStream("node1", 9998);
        DataStream<Tuple3<String, String, String>> stream2 = source2.map(s -> {
            String[] split = s.split(" ");
            return Tuple3.of(split[0], split[1], split[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });
        stream2.print("stream2: ");
        env.execute();
    }
}
