package org.flink.demos.source_function;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


import java.util.List;

public class Demo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.fromElements(
                "{\"uid\":1,\"name\":\"张三\",\"friends\":[{\"uid\":2,\"name\":\"李四\"},{\"uid\":10,\"name\":\"王五\"},{\"uid\":4,\"name\":\"jj\"}]}",
                "{\"uid\":5,\"name\":\"小明\",\"friends\":[{\"uid\":6,\"name\":\"马润\"},{\"uid\":7,\"name\":\"麦克\"}]}",
                "{\"uid\":9,\"name\":\"马化腾\",\"friends\":[{\"uid\":2,\"name\":\"马云\"},{\"uid\":3,\"name\":\"王健林\"}]}");
        DataStream<UserInfo> map = source.map(json -> JSON.parseObject(json, UserInfo.class));

        DataStream<Tuple5<Integer, String, Integer, String, Integer>> flatMap = map.flatMap(new FlatMapFunction<UserInfo, Tuple5<Integer, String, Integer, String, Integer>>() {
            @Override
            public void flatMap(UserInfo userInfo, Collector<Tuple5<Integer, String, Integer, String, Integer>> collector) throws Exception {

                for (Friend friend : userInfo.getFriends()) {
                    Tuple5<Integer, String, Integer, String, Integer> t5 = Tuple5.of(userInfo.getUid(), userInfo.getName(), friend.getUid(), friend.getName(), 1);
                    collector.collect(t5);
                }
            }
        });

        flatMap.keyBy(key -> key.f0).reduce(new ReduceFunction<Tuple5<Integer, String, Integer, String, Integer>>() {
            @Override
            public Tuple5<Integer, String, Integer, String, Integer> reduce(Tuple5<Integer, String, Integer, String, Integer> last, Tuple5<Integer, String, Integer, String, Integer> cur) throws Exception {
                last.f4++;
                cur.f4=last.f4;
                return cur;
            }
        }).print();

        env.execute();


    }
}

@Data
class UserInfo{
    private int uid;
    private String name;
    private List<Friend> friends;
}

@Data
class Friend{
    private int uid;
    private String name;
}