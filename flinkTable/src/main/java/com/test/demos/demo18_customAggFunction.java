package com.test.demos;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

public class demo18_customAggFunction {

    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Table table = env.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.INT())
                ),
                Row.of("001","aaa",20),
                Row.of("002","abc",30),
                Row.of("003","ddd",50),
                Row.of("001","aaa",50)
        );
        env.executeSql("drop table if exists t1");
        env.createTemporaryView("t1",table);
        env.createTemporarySystemFunction("myAvg", myAvg.class);

        env.executeSql("select name,myAvg(score) as avg_s from t1 group by name").print();
    }


    // 记录中间值的累加器
    // mutable accumulator of structured type for the aggregate function
    public static class WeightedAvgAccumulator {
        public double sum = 0.0;
        public int count = 0;
    }

    // function that takes (value BIGINT, weight INT), stores intermediate results in a structured
    // type of WeightedAvgAccumulator, and returns the weighted average as BIGINT
    public static class myAvg extends AggregateFunction<Double, WeightedAvgAccumulator> {

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            //创建累加器
            return new WeightedAvgAccumulator();
        }

        @Override
        public Double getValue(WeightedAvgAccumulator acc) {
            // 获取累加器的值
            if (acc.count == 0) {
                return null;
            } else {
                return  acc.sum / acc.count;
            }
        }

        public void accumulate(WeightedAvgAccumulator acc,Integer iWeight) {
            acc.sum += iWeight;
            acc.count += 1;
        }

        // 这个是实现回撤流的
        public void retract(WeightedAvgAccumulator acc, Integer iWeight) {
            acc.sum -= iWeight;
            acc.count -= 1;
        }

        public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it) {
            for (WeightedAvgAccumulator a : it) {
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }

        public void resetAccumulator(WeightedAvgAccumulator acc) {
            acc.count = 0;
            acc.sum = 0.0;
        }
    }

}
