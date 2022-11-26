package org.flink.demos.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 这是没有加入状态时的例子，
 * 程序异常退出，重新执行时，从零开始
 * @author chnejia
 */

public class demo1 {
    public static void main(String[] args) throws Exception {
        System.setProperty("log.file","./webui");
        Configuration conf = new Configuration();
        conf.setString("web.log.path","./webui");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("up01", 9998);
        SingleOutputStreamOperator<String> map = source.map(new MapFunction<String, String>() {
            String acc = "";

            @Override
            public String map(String s) throws Exception {
                acc = acc + s;
                return acc;
            }
        });

        map.print();
        env.execute();
    }
}
