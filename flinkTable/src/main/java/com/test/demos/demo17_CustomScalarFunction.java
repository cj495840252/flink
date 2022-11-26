package com.test.demos;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import javax.xml.crypto.Data;

public class demo17_CustomScalarFunction {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Table table = env.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),
                Row.of("aaa"),
                Row.of("abc"),
                Row.of("aDc")
        );

        env.createTemporaryView("t1",table);
        env.createTemporarySystemFunction("myupper",MyUpper.class);

        env.executeSql("select name,myupper(name) as upname from t1").print();
    }


    public static class MyUpper extends ScalarFunction{
        public String eval(String str){
            return str.toUpperCase();
        }
    }
}
