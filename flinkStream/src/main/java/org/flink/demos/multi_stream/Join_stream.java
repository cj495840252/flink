package org.flink.demos.multi_stream;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Join_stream {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");
        env.setParallelism(1);

        DataStreamSource<String> source1 = env.socketTextStream("node1", 9999);
        DataStream<Tuple2<String, String>> stream1 = source1.map(s -> {
            String[] split = s.split(" ");
            return Tuple2.of(split[0], split[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });
        stream1.print("stream1: ");


        DataStreamSource<String> source2 = env.socketTextStream("node1", 9998);
        DataStream<Tuple3<String, String, String>> stream2 = source2.map(s -> {
            String[] split = s.split(" ");
            return Tuple3.of(split[0], split[1], split[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });
        stream2.print("stream2: ");


        JoinedStreams<Tuple2<String, String>, Tuple3<String, String, String>> join = stream1.join(stream2);
        DataStream<String> stream = join.where(s1 -> s1.f0)
                .equalTo(s2 -> s2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                    @Override
                    public String join(Tuple2<String, String> t1, Tuple3<String, String, String> t2) throws Exception {
                        return t1.f0 + t1.f1 + t2.f1 + t2.f2;
                    }
                });

        stream.print();

        env.execute();
    }
}
