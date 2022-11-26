package com.test.demos;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Locale;

public class demo21_metricsCounter {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8081);
        configuration.setString("web.log.path","F:\\JavaProject\\flink-test\\logs");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        DataStreamSource<String> source = env.socketTextStream("node1", 9999);

        source.process(new ProcessFunction<String, String>() {
            LongCounter myCounter;
            MyGuage gauge;
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                // 业务逻辑之外的计数,比如json解析一个字符串，try catch。解析失败count+1。
                myCounter.add(1);
                gauge.add(1);
                collector.collect(s.toUpperCase());


            }

            @Override
            public void open(Configuration parameters) throws Exception {
                myCounter = getRuntimeContext().getLongCounter("myCounter");
                gauge = getRuntimeContext().getMetricGroup().gauge("myGauge", new MyGuage());
            }
        }).print();

        env.execute();
    }


    public static class MyGuage implements Gauge<Integer>{
        int recordCount = 0;
        public void add(int i){
            recordCount+=i;
        }
        @Override
        public Integer getValue() {
            return recordCount;
        }
    }

}
