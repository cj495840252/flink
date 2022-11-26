package com.test.demos;

import org.apache.flink.table.api.*;


import static org.apache.flink.table.api.Expressions.$;

public class demo2_table {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Table table = env.from(TableDescriptor
                .forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build())
                .format("json")
                .option("topic", "json")
                .option("properties.bootstrap.servers", "node2:9092")
                .option("properties.group.id", "testGroup")
                .option("scan.startup.mode", "earliest-offset")
                .option("json.fail-on-missing-field", "false")
                .option("json.ignore-parse-errors", "true")
                .build());


        //table.execute().print();//直接execute相当于select*from table
        Table table1 = table.groupBy($("gender"))
                .select($("gender"),$("age").avg().as("avg"));

        table1.execute().print();
    }
}
