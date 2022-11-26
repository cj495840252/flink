package org.flink.demos.worldcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;




public class WordCount_Web {
    public static void main(String[] args) throws Exception {

        // 带web环境的
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 创建一个编程入口环境,本地运行时默认的并行度为逻辑核数·
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 修改默认并行度

        DataStreamSource<String> source = env.socketTextStream("192.168.88.161",9999);
        DataStream<Tuple2<String,Integer>> words = source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception{
                String[] split = s.split("\\s+");
                for(String word:split){
                    collector.collect(Tuple2.of(word,1));
                }
            }
        }).setParallelism(4); //每个算子可以自己设置并行度

        KeyedStream<Tuple2<String,Integer>,String> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String,Integer>> resultStream = keyed.sum(1);// 或者字段“f1”
        resultStream.print("wcSink");
        words.print("words");

        env.execute();
    }
}