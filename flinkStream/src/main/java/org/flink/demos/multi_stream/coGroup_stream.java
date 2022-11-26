package org.flink.demos.multi_stream;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class coGroup_stream {
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

        CoGroupedStreams<Tuple2<String, String>, Tuple3<String, String, String>> coGroup = stream1.coGroup(stream2);
        DataStream<Tuple4<Integer, String, String, String>> stream = coGroup
                .where(tp -> tp.f0)
                .equalTo(tp -> tp.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<Integer, String, String, String>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> iterable, Iterable<Tuple3<String, String, String>> iterable1, Collector<Tuple4<Integer, String, String, String>> collector) throws Exception {
                        //两个迭代器，第一个是流1的数据，第二个是流2的数据，且f0字段相等
                        // 实现左外连接
                        Tuple4<Integer, String, String, String> tuple4 = new Tuple4<>();

                        for (Tuple2<String, String> tuple2 : iterable) {
                            boolean flag = false;
                            for (Tuple3<String, String, String> tuple3 : iterable1) {
                                flag = true;
                                tuple4.setField(Integer.parseInt(tuple2.f0), 0);
                                tuple4.setField(tuple2.f1, 1);
                                tuple4.setField(tuple3.f1, 2);
                                tuple4.setField(tuple3.f2, 3);
                                collector.collect(tuple4);
                            }
                            if (!flag) {
                                tuple4.setField(Integer.parseInt(tuple2.f0), 0);
                                tuple4.setField(tuple2.f1, 1);
                                tuple4.setField(null, 2);
                                tuple4.setField(null, 3);
                                collector.collect(tuple4);
                            }
                        }


                    }
                });

        stream.print("Go: ");
        env.execute();
    }
}
