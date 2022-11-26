package org.flink.demos.state;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;
import java.time.Duration;

public class demo6_checkPoint {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.开启checkpoint，时间间隔3000ms，语义为exactly-once
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        // 2.指定checkpoint的存储位置
        env.getCheckpointConfig().setCheckpointStorage(new Path("hdfs://node1:8020/checkpoint/"));
        // 3.运行checkpoint失败的最大次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        // 4.checkpoint的算法模式, 是否需要对齐
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 5.job取消时是否保留checkpoint的数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 6.设置checkpoint对齐的超时时间
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofMillis(2000));
        // 7.两次checkpoint的最小时间间隔,和构造时一样
        env.getCheckpointConfig().setCheckpointInterval(2000);
        // 8.最大并行的checkpoint数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);
        // 9.要用状态时，最后指定状态后端
        env.setStateBackend(new EmbeddedRocksDBStateBackend());


        // 策略1：task失败自动重启策略
        // 固定延迟重启，参数1：故障重启最大次数；参数2：两次重启的间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,2000));
        // 策略2：默认的配置方式，不重启，一个出错直接job失败
        env.setRestartStrategy(RestartStrategies.noRestart());



        /*  策略3：：故障越频繁，两次重启间隔越长
            Time initialBackoff         重启间隔惩罚时长的初始值，1s
            Time maxBackoff             重启间隔的最大时长，60s
            double backoffMultiplier    重启时间间隔时长的惩罚倍数，2，每多故障一次，重启时间就在上一次故障惩罚时间上*2
            Time resetBackoffThreshold  重置惩罚：平稳运行时长阈值，平稳运行达到这个阈值后，如果在故障，则故障重启延迟时间重置，1s
            double jitterFactor         取一个随机数，夹在重启时间上，防止大量task同时重启
        */
        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
                Time.seconds(1),
                Time.seconds(60),
                2.0,
                Time.seconds(1),
                1.0
        ));

        /*策略4
        * int maxFailureRate;                   指定时间内的最大失败次数
        * Time failureInterval;                 指定的衡量时长
        * Time delayBetweenAttemptsInterval;    两次重启之间的时间间隔
        * */
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5,Time.hours(1),Time.seconds(5)));


        /*
        * 策略5：自定义，本策略就是退回到配置文件所配置的策略
        * 常用与自定义RestartStrategy
        * 用户自定义了重启策略，而且配置在了flink-config.yaml文件中
        * */
        env.setRestartStrategy(RestartStrategies.fallBackRestart());



        /* Failover-strategy 策略
         本参数的含义是，当一个task失败时，需要restart的时候，是restart整个job中的所有task还是之restart一部分task
         RestartAll
         划分为Region，重启相关的region，一般为pipeline
         */

        
        // Job失败重启,这一段需要放在env创建之前
        Configuration conf = new Configuration();
        conf.setString("execution.savepoint.path","./flink-test/mode1/src/outfile/checkpoint/0c7ab838c5ff55621fd6130d66528099/chk-188");

    }
}
