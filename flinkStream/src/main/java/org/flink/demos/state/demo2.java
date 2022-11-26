package org.flink.demos.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;


/**
 * 算子状态，通常用于source记录偏移量
 * @author chnejia
 */

public class demo2 {
    public static void main(String[] args) throws Exception {
        System.setProperty("log.file","./webui");
        Configuration conf = new Configuration();
        conf.setString("web.log.path","./webui");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // 开启checkpoint，默认精确一次
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointStorage(new URI("hdfs://test"));
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");


        // task级别的failover 默认不重启
        // env.setRestartStrategy(RestartStrategies.noRestart());
        // 固定重启上限，两次重启之间的间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));


        DataStreamSource<String> source = env.socketTextStream("up01", 9998);
        SingleOutputStreamOperator<String> map = source.map(new StateMapFunction());

        map.print();
        env.execute();
    }
}

class StateMapFunction implements MapFunction<String,String>, CheckpointedFunction{
    ListState<String> listState;
    @Override
    public String map(String s) throws Exception {
        listState.add(s);
        Iterable<String> strings = listState.get();
        StringBuilder builder = new StringBuilder();
        for (String string : strings) {
            builder.append(string);
        }
        return builder.toString();
    }

    /**
     * 系统对状态数据做快照是会调用的方法，在持久化前做一些操控
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("checkpoint： "+context.getCheckpointId());
    }

    /**
     * 算子在启动之处，会调用下面的方法，来为用户做数据的持久化
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore store = context.getOperatorStateStore();

        // 定义一个状态的描述器
        ListStateDescriptor<String> stateName = new ListStateDescriptor<>("state_name", String.class);
        //算子状态中，只有这一个getListState可以用。他会在task失败后，task自动重启时，会帮用户加载最近一次快照
        // 若job重启，则不会加载
         listState = store.getListState(stateName);

        /**
         * unionListState和普通ListState的区别
         * unionListState的快照存储系统，在系统重新启动后，加载数据，需要人工指定状态数据的重复配
         * ListState，在系统重启后，由系统自动进行状态数据的重分配
         */
        store.getUnionListState(stateName);

    }
}