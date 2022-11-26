package org.flink.demos.worldcount;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


import org.apache.flink.api.java.operators.*;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount_Batch1 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataSet<String> source = environment.fromElements("chenjia", "chenjia zhangsan", "lishi lishi");
        DataSet<String> flatMap = source.flatMap((String s, Collector<String> collector) -> {
                String[] words = s.split("\\s+");
                for (String word : words) {
                    collector.collect(word);
                }
            }).returns(Types.STRING);
        DataSet<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s,1);
            }
        });

        UnsortedGrouping<Tuple2<String, Integer>> groupBy = map.groupBy(0);

        DataSet<Tuple2<String, Integer>> reduce = groupBy.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tuple2, Tuple2<String, Integer> t1) throws Exception {

                return Tuple2.of(t1.f0, t1.f1+tuple2.f1);
            }
        });

        //只能取0和1，表示取Tuple第几个
        DataSet<Tuple> project = reduce.project(0);
        project.print();
//        UnsortedGrouping<Tuple2<String, Integer>> groupBy = map.groupBy(0);
//        AggregateOperator<Tuple2<String, Integer>> sum = groupBy.sum(1);
//        sum.print();
    }
}
