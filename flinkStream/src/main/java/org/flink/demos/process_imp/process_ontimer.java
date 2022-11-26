package org.flink.demos.process_imp;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.flink.demos.window.EventBean;

public class process_ontimer {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");
        env.setParallelism(1);

        DataStreamSource<String> source1 = env.socketTextStream("node1", 9999);
        KeyedStream<EventBean, String> keyBy = source1.map(s -> {
            String[] split = s.split(",");
            return new EventBean(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3]);
        }).keyBy(EventBean::getId);

        SingleOutputStreamOperator<String> process = keyBy.process(new KeyedProcessFunction<String, EventBean, String>() {
            ValueState<Long> timerstate;

            @Override
            public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, String>.Context context, Collector<String> collector) throws Exception {
                if (eventBean.getMsg().equals("submit_order")) {
                    long timerTime = context.timerService().currentProcessingTime() + 30 * 1000L;
                    context.timerService().registerProcessingTimeTimer(timerTime);
                    timerstate.update(timerTime);
                    collector.collect("检测到用户：" + eventBean.getId() + "下单，注册定时器" + timerstate.value());
                }

                if (eventBean.getMsg().equals("payOrder")) {
                    context.timerService().deleteProcessingTimeTimer(timerstate.value());
                    collector.collect("检查到用户" + eventBean.getId() + "在下单后30s已经支付，取消定时器" + timerstate.value());

                }

            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, EventBean, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                String guid = ctx.getCurrentKey();
                out.collect("用户：" + guid + ",您的订单快超时了，赶紧支付咩");
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                timerstate = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerstate", Long.class));
            }
        });
        env.execute();
    }
}
