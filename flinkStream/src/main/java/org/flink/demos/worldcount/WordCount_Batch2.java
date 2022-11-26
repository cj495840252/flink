package org.flink.demos.worldcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount_Batch2 {
    public static void main(String[] args) throws Exception {
        // 创建环境
        ExecutionEnvironment batchenv = ExecutionEnvironment.getExecutionEnvironment();


        // 读数据
        DataSource<String> dataSource = batchenv.readTextFile("F:\\JavaProject\\flink-test\\mode1\\words");
        dataSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split("\\s+");
                for (String word : words) {
                    if (word.length()>0)
                        collector.collect(Tuple2.of(word,1));
                }
            }
        }).groupBy(0).sum(1).print();//或者f0
    }
}
