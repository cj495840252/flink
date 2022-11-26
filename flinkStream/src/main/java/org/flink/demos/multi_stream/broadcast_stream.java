package org.flink.demos.multi_stream;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class broadcast_stream {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");
        env.setParallelism(1);

        // 1.获取两个流
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

        // 2.将第二条流转成广播流
        // context中记录状态，相当于获取一个map，用来放数据用，key，value类型。
        MapStateDescriptor<String, Tuple2<String, String>> userInfoStatusDesc =
                new MapStateDescriptor<>("userInfoStatus",
                TypeInformation.of(String.class),
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                }));
        BroadcastStream<Tuple3<String, String, String>> broadcast = stream2.broadcast(userInfoStatusDesc);

        // 3.流1 连接广播流
        BroadcastConnectedStream<Tuple2<String, String>, Tuple3<String, String, String>> connect = stream1.connect(broadcast);

        // 4.两个流的操作，最后返回一条流
        // 连接广播流后只有一个方法，集process能用，
        // process中有两种实现，第一个带keyd，一个不带keyd，如下；当keyd流时则用keyd
        SingleOutputStreamOperator<String> process =
                connect.process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
            @Override
            public void processElement(Tuple2<String, String> data,
                                       BroadcastProcessFunction<Tuple2<String, String>,
                                               Tuple3<String, String, String>, String>.ReadOnlyContext readOnlyContext,
                                       Collector<String> collector) throws Exception {
                // 处理主流中的数据，来一条处理一条
                ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState = readOnlyContext.getBroadcastState(userInfoStatusDesc);
                if (broadcastState!=null){
                    // 这里不要去broadcastState中操作，避免修改
                    Tuple2<String, String> userInfo = broadcastState.get(data.f0);
                    collector.collect(data.f0+" "+data.f1+" "+(userInfo==null?null:userInfo.f0)+" "+(userInfo==null?null:userInfo.f1));
                } else {
                    collector.collect(data.f0+" "+data.f1+" null null");
                }

            }

            @Override
            public void processBroadcastElement(Tuple3<String, String, String> tuple3,
                                                BroadcastProcessFunction<Tuple2<String, String>,
                                                        Tuple3<String, String, String>, String>.Context context,
                                                Collector<String> collector) throws Exception {
                // 将获得的广播流拆分，装到上下文中的容器中
                BroadcastState<String, Tuple2<String, String>> state = context.getBroadcastState(userInfoStatusDesc);
                state.put(tuple3.f0, Tuple2.of(tuple3.f1,tuple3.f2));
            }
        });

        // 5.输出流，查看结果
        process.print();
        env.execute();
    }
}
