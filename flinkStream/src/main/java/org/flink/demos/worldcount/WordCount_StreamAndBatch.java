package org.flink.demos.worldcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class WordCount_StreamAndBatch {
    public static void main(String[] args) throws Exception {
        // 创建一个编程入口

        // 流式处理入口环境
        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.getExecutionEnvironment();
        envStream.setParallelism(1);
        envStream.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // 读文件 得到datestream
        DataStreamSource<String> streamSource = envStream.readTextFile("F:\\JavaProject\\flink-test\\mode1\\words");

        // 调用dataStream的算子计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = streamSource.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                String[] words = s.split("\\s+");
                for (String word : words) {
                    if (word.length() > 0)
                        collector.collect(Tuple2.of(word, 1));
                }
        })
                //.returns(new TypeHint<Tuple2<String, Integer>>() {});
                //.returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
                .returns(Types.TUPLE(Types.STRING,Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyBy = flatMap.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }
        );

        keyBy.sum(1).print();

        envStream.execute();
    }
}
