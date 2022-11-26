package org.flink.demos.multi_stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class connect_stream {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");
        env.setParallelism(1);

        DataStreamSource<String> source1 = env.socketTextStream("node1", 9999);
        DataStreamSource<String> source2 = env.socketTextStream("node1", 9998);
        ConnectedStreams<String, String> connect = source1.connect(source2);
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, String, String>() {
            // map完成后会返回成一个流
            final String prefix = "Pre_";

            @Override
            public String map1(String s) throws Exception {
                //第一个流map逻辑，假设全是数字，乘10加前缀
                return prefix + (Integer.parseInt(s) * 10);
            }

            @Override
            public String map2(String s) throws Exception {
                //第二个流，全部转大写，加前缀
                return prefix + s.toUpperCase();
            }
        });
        map.print();


        env.execute();
    }
}
