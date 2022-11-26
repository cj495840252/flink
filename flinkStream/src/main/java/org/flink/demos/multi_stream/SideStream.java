package org.flink.demos.multi_stream;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.flink.demos.source_function.init.EventLog;
import org.flink.demos.source_function.init.MySourceFunction;

public class SideStream {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");
        env.setParallelism(1);

        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());
//        source.print();

        SingleOutputStreamOperator<EventLog> process = source.process(new ProcessFunction<EventLog, EventLog>() {
            @Override
            public void processElement(EventLog eventLog, ProcessFunction<EventLog, EventLog>.Context context, Collector<EventLog> collector) throws Exception {
                /*
                 * EventLog：输出数据
                 * Context：上下文对象
                 * Collector：收集器
                 * */
                String eventId = eventLog.getEventId();
                if ("appLaunch".equals(eventId)) {
                    context.output(new OutputTag<EventLog>("appLaunch",TypeInformation.of(EventLog.class)), eventLog);
                } else if ("addClick".equals(eventId)) {
                    context.output(new OutputTag<String>("addClick",TypeInformation.of(String.class)), JSON.toJSONString(eventLog));
                } else {
                    collector.collect(eventLog);
                }
            }
        });

        DataStream<EventLog> launch = process.getSideOutput(new OutputTag<EventLog>("appLaunch", TypeInformation.of(EventLog.class)));
        DataStream<String> click = process.getSideOutput(new OutputTag<String>("addClick",TypeInformation.of(String.class)));
        launch.print("launch");
        click.print("click");
        process.print("main");
        env.execute();
    }
}
