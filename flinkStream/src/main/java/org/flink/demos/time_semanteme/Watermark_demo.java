package org.flink.demos.time_semanteme;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.flink.demos.avroschema.EventBean;

import java.time.Duration;

public class Watermark_demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("up01", 9999);

        // 策略1：WatermarkStrategy.noWatermarks();不生成
        // 策略2：WatermarkStrategy.forMonotonousTimestamps 紧跟最大时间，常用
        // 策略3：WatermarkStrategy.forBoundedOutOfOrderness  允许一定时间的乱序，常用
        // 策略4：WatermarkStrategy.forGenerator 自定义
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());
        stream.assignTimestampsAndWatermarks(
                //这一行的泛型和数据类型一致
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                        // 下面这个获取初始时间戳
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String s, long l) {
                                // 方法中抽取时间戳返回，假如数据是这样的字符串，1,e0,168673487846,pg01
                                return Long.parseLong(s.split(",")[2]);
                            }
                        })
        );

        // 策略2
        stream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMillis(100))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String s, long l) {
                        return 100;
                    }
                }));


        SingleOutputStreamOperator<String> process = stream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                context.timestamp();// 该条数据中记录的时间
                context.timerService().currentWatermark();//此刻watermark
                context.timerService().currentProcessingTime();// 此刻process的处理时间
                collector.collect(s);
            }
        });

//        stream.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<String>() {
//            @Override
//            public WatermarkGenerator<String> createWatermarkGenerator(Context context) {
//                return null;
//            }
//        }));
        process.print();
        env.execute();

    }
}
