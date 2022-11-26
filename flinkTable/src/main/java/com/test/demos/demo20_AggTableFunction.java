package com.test.demos;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 *自定义表聚合函数，求topN
 *
 * 1,male,zs,88
 * 2,male,bb,99
 * 3,male,cc,76
 * 4,female,aa,78
 * 5,female,dd,92
 * 6,female,ww,86
 *
 * 求每种性别中。分数最高的两个学生，不用row_number方式
 *
 * select
 *      gender,
 *      func(score,2)
 * from t
 * group by gender
 *
 */


public class demo20_AggTableFunction {

    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Table table = env.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.INT()),
                        DataTypes.FIELD("gender",DataTypes.STRING())
                ),
                Row.of("1","aaa",88, "male"),
                Row.of("2","bbb",99, "male"),
                Row.of("3","ccc",76, "male"),
                Row.of("4","ddd",78, "female"),
                Row.of("5","eee",92, "female"),
                Row.of("6","fff",86, "female")
        );
        //env.executeSql("drop table if exists t1");
        //env.createTemporaryView("t1",table);
        table.groupBy($("gender"))
             .flatAggregate(call(Top2.class, $("score")))
             .select($("gender"), $("score_top"), $("rank"))
             .execute().print();
    }


    // mutable accumulator of structured type for the aggregate function
    public static class Top2Accumulator {
        public Integer first;
        public Integer second;
    }

    // function that takes (value INT), stores intermediate results in a structured
    // type of Top2Accumulator, and returns the result as a structured type of Tuple2<Integer, Integer>
    // for value and rank
    // 这个注解限制返回值的类型，若不加注解。可以返回元组类型数据，用f0,f1获取返回的值
    @FunctionHint(output = @DataTypeHint("ROW<score_top INT, rank INT>"))
    public static class Top2 extends TableAggregateFunction<Row, Top2Accumulator> {

        //first,second记录score分数第一和第二大的两个值
        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        public void accumulate(Top2Accumulator acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        // 第一个参数为当前得acc，第二个参数为其他并行的迭代器，最后汇总的算法
        public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
            for (Top2Accumulator otherAcc : it) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        // 输出结果，collector中的泛型
        public void emitValue(Top2Accumulator acc, Collector<Row> out) {
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
                //out.collect(Tuple2.of(acc.first, 1));
                out.collect(Row.of(acc.first,1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Row.of(acc.second,2));
            }
        }
    }

}
