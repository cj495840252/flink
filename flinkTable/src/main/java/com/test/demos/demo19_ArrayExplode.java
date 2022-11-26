package com.test.demos;

import org.apache.flink.table.api.*;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;

public class demo19_ArrayExplode {
    public static void main(String[] args) {

        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Table table = env.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.INT()),
                        DataTypes.FIELD("course",DataTypes.ARRAY(DataTypes.STRING()))
                ),
                Row.of("001","aaa",20, Expressions.array("数学","语文","英语"))
        );
        env.executeSql("drop table if exists t1");
        env.createTemporaryView("t1",table);
        env.executeSql("select t1.name,t2.course from t1 cross join unnest(t1.course) as t2(course)").print();
    }

}
