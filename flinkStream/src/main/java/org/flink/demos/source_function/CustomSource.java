package org.flink.demos.source_function;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.flink.demos.source_function.init.EventLog;
import org.flink.demos.source_function.init.MySourceFunction;

public class CustomSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());
        source.map(JSON::toJSONString).print();
        env.execute();



    }
}


