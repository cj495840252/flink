package com.test.demos;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

//自定义实现，数组展开
public class demo19_TableFunction {
    public static void main(String[] args) {

        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Table table = env.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.INT()),
                        DataTypes.FIELD("course",DataTypes.STRING())
                ),
                Row.of("001","aaa",20, "语文 数学 英语")
        );
        env.executeSql("drop table if exists t1");
        env.createTemporaryView("t1",table);

        env.createTemporarySystemFunction("mysplit",Mysplit.class);
        env.executeSql("select id,name,score,t2.course,nums from t1 left join lateral table(mysplit(t1.course,' ')) as t2(course,nums) on true").print();
    }

    @FunctionHint(output = @DataTypeHint("ROW<course STRING, nums INT>"))
    public static class  Mysplit extends TableFunction<Row>{
        public void eval(String str,String delimiter){
            for(String s: str.split(delimiter)){
                collect(Row.of(s,s.length()));
            }
        }
    }

}
