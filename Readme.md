# Flink

数据流上的有状态计算

监控端口8081

历史记录8082

## shell

```shell
nc  -lk  9999 监听某个端口

集群运行时手动触发savepoint
flink savepoint -t remote :jobId [:targetDirectory]
-t: 指定flink的运行模式，remote提交到standlone，默认yarn

# 为yarn模式集群触发savepoint
flink savepoint :jobId [:targetDirectory] -yid :yarnId

# 停止一个job集群，并触发savepoint
flink stop --savepointPath [:targetDirectory] ：jobId

# 从一个指定savepoint恢复启动job集群
flink run -s :savepointPath [:runArgs]
-----------------------------------------
flink run 
-c org......ClassName 	入口类
-p 5 					并行度
-s hdfs://node1:8020/	从保存点恢复时指定
java.jar				jar包
-------------------------------------------
```



## 安装模式

- local

  多线程去模拟各个工作

- standalone

- standalone-HA 高可用

- Flink-on-Yarn

  **两种模式**

  1. session模式

     在yarn集群中启动了一个Flink集群，并重复使用

     特点：需要事先申请资源，启动JobManager和TaskManager

     优点：提高执行效率

     缺点：作业执行完成后不会释放资源

     适用于作业提交频繁的场景，小作业多的场景

     

  2. Per-Job模式

     - 用的更多，流式运行起来就不会停

     针对每一个任务都启动一个独立的flink集群，不能重复使用该集群

     特点：每次递交作业都需要申请一次资源

     优点：作业完成，资源立即释放，不会占用系统资源

     缺点：每次递交作业都需要申请资源，影响执行效率

     适用于作业少，大作业场景

  ```shell
  # 提交flink任务
  bin/flink run jar包 --input 数据文件地址 --output 输出地址
  
  第一种Session模式：
  # yarn上启动一个Flink session
  yarn-session.sh -n 2 -tm 800 -s 1 -d
  申请两个cpu，1600m内存
  # -n表示申请两个容器，这里值的就是多少个taskmanager
  # -tm 表示每个TaskManger的内存大小
  # -s表示每个TaskManger的slots数量
  # -d表示以后台程序方式运行
  
  yarn application -kill 应用名
  # 查看运行结果，保存在log目录下
  tail log/flink-*-taskexecutor-*.out
  
  第二种Job分离模式
  flink run -m yarn-cluser-yjm 1024 jar地址
  # -m jobmanager的地址
  # -yjm 1024  指定jobmanager的内存信息
  # -ytm 1024  指定taskmanager的内存信息
  ```

  

  ![](C:\Users\49584\Desktop\Java\picture\flink-on-yarn.png)



## 编程模型

<img src="C:\Users\49584\Desktop\Java\picture\flink编程模型.png" style="zoom:60%;" />

## webUi

```java
<dependency>
	<groupId>org.apache.flink</groupId>
		<artifactId>flink-runtime-web_2.12</artifactId>
	<version>1.14.4</version>
</dependency>

 // 带web环境的环境创建
Configuration conf = new Configuration();
conf.setInteger("rest.port",8081);
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);//自动转换
```

## 基础概念

### JobManager

### TaskManager

TaskManager相当于整个集群的Slave节点，负责具体的任务执行和对应任务在每个节点上的资源申请与管理

TaskManager是一个JVM进程，他的内存资源划分成多个slot，每个slot可以运行一个线程（即subtask）

### Chain

Operator Chain：算子链

在一个task中串行执行多个算子

每一个算子都可以成为独立的task

### Task 

在ui界面可以看到划分的task，一个task可以包含多个算子，task的每个线程称作subtask，subtask=并行度

Task是可以多个进程，可以在不同的机器上，subtask可以在不同的进程，机器上

### Slot

槽位

- 主要用来隔离内存，cpu是共享的

- 每台机器一个TaskManager，TaskManager提供了固定个数slot槽位，可以手动配置

- job中只要有一个task的并行度 > slot个数，那么job提交失败

    

![](C:\Users\49584\Desktop\Java\picture\Process-task.png)

### 并行度

上下游算子能否chain在一起，放在一个task中

- 上下游实例之前，one To one数据传输
- 并行度相同
- 属于相同的共享组

**共享组**相同的算子可以放在一个槽位中

```java
算子.setSharing("组id");//设置该算子共享组，下游算子默认继承上一个算子的组
算子.setParallelism();// 设置并行度
算子.disableChaining();//禁止前后链绑定该算子
算子.startNewChain();//对算子开启新链
```



### checkpoint

#### 配置

```java
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

```

#### 两阶段流程

![](C:\Users\49584\Desktop\Java\picture\checkpoint两阶段流程.png)

#### 精确一次

> - source端保证
>
>     1. 能够记录偏移量，
>     2. 能够重放数据
>     3. 将偏移量记录在state中，与下游算子的其他state一起，经由checkpoint机制实现状态数据的快照统一
>
> - 内部状态
>
>     Checkpoint机制，把状态数据定期持久化存储下来，程序发生故障后，可以选择状态点恢复，避免数据丢失和重复
>
>     基于分布式快照算法，flink实现了整个数据流中各算子的状态数据快照统一，
>
>     即：一次checkpoint后所持久化的各算子状态数据，确保时经过了相同数据的影响
>
>     - barrier对其的checkpoint，可支持exactly-once
>     - barrier非对齐的checkpoint，只能实现at least once
>
> - sink端
>
>     flink将数据发送到sink端时，通过两阶段提交协议，即TwoPhaseCommitSinkFunction函数，该si你看Function函数提取并封装了两阶段提交协议中的公共逻辑，保证Flink发送Sink端实现严格一次处理语义。同时sink端必须支持事务机制，能够进行数据回滚或者满足幂等性
>
>     > `回滚机制`：即当作业失败后，能够将部分写入的结果回滚到之前写入的状态
>     >
>     > `幂等性`：就是一个相同的操作，无论重复多少次，造成的结果和只操作一次相等。即作业失败后写入部分结果，但是重新写入全部时，不会产生错误结果或负面影响
>
>     1. 采用幂等写入方式
>     2. 两阶段提交写入方式，2PC
>     3. 采用预写日志2pc提交方式

##### sink写出容错

> 两阶段事务写入：利用了checkpoint两阶段提交协议，和目标存储系统的事务支持机制
>
> 1. sink算子在一批数据处理过程中，先通过预提交事务对外输出数据
> 2. 待这批数据收到checkpoint信号后，向checkpoint coordinator上报自身checkpoint完成信息
> 3. checkpoint coordinator收到所有算子的checkpoint完成信息后，再向各算子广播本次checkpoint完成信息
> 4. 两阶段事务提交sink算子收到checkpoint coordinator回调信息，执行commit操作

1. 两阶段事务提交

    需要外部支持事务，这两种都属于两阶段事务写入

2. 两阶段日志提交

    在flink中模拟事务

3. 幂等性写入

    能实现最终一致，但有可能过程中不一致。因为每条数据数据写入时，产生的数据可能不相同，比如写入时加入随机数

    `kafka不支持幂等性，但又幂等机制`

**2PCSink**

> sink的实现流程图，比如支持事务的jdbcSink

![](C:\Users\49584\Desktop\Java\picture\checkpoint两阶段流程2.png)

**预写日志**

> 借用sink的状态，当battier来了后，发送出去

#### barrier

1. job Manager会向数据中发送barrier，
2. 当算子遇到barrier时，该算子触发checkpoint，
3. 算子快照完成后返回ack应答
4. 当所有job Manager确认所有算子完成后会给算子发送callBack

#### 对齐问题

> 当一个算子接收两条流时，barrier到达时间不统一。产生了对齐的checkpoint和非对齐的checkpoint
>
> 1. 对齐的checkpoint
>
>     barrier先到的数据流添加到缓存，不做处理。等慢的barrier来到后，异步快照。然后优先处理积压的数据
>
> 2. 非对其checkpoint
>
>     不能实现精确一致，只能保证atListOnce。快的barrier到达时，直接做快照，将未输出的数据也保存下来



### 失败重启

#### Job级

在idea中测试从某个save point中恢复

```java
// Job失败重启,这一段需要放在env创建之前
Configuration conf = new Configuration();
conf.setString("execution.savepoint.path","./mode1/src/outfile/checkpoint/0c7ab838c5ff55621fd6130d66528099/chk-188");
```



#### Task级



## datastream

datastream有很多类型

<div align="left">
    <img src="C:\Users\49584\Desktop\Java\picture\dataStreamClass.png" style="zoom:100%;" />
</div>

datastream：基本流

KeyedStream：keyby后返回，reduce，fold，sum，max等聚合后返回基本流

WindowedStream：在KeyedStream的基础上调用window()返回

AllWindowedStream：没有分组，直接window(),返回该流。聚合后返回基本流

ConnectedStream：connect()协同后返回该流，map和flatmap后返回基本流

JoinedStream：join()后返回，window().apply()方法后返回基本流

CoGroupStream：coGroup()后返回，window().apply()方法后返回基本流

### process

> 在不同类型的流上调用不同的process()需要传入的process Function不同
>
> process方法自由度最大，map，flatmap等都是基于process实现
>
> - 里面可以使用控制生命周期的open和close方法

#### ProcessFunction

```java
SingleOutputStreamOperator<Tuple2<String, String>> process = source1.process(new ProcessFunction<String, Tuple2<String, String>>() {
    @Override
    public void processElement(String s, ProcessFunction<String, Tuple2<String, String>>.Context context, Collector<Tuple2<String, String>> collector) throws Exception {
        /*该方法是必须要实现的
        * s:传入的数据
        * context:上下文对象，里面封装了当前的运行环境
        * collector:收集器，每一个返回的数据由它收集
        * */
        String[] split = s.split("\\s+");
        collector.collect(Tuple2.of(split[0],split[1]));

        //侧流输出
        context.output(new OutputTag<>("侧流1", Types.STRING),s);

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 可以使用生命周期方法Open和Close，得到很多上下文信息
        RuntimeContext runtimeContext = getRuntimeContext();
        runtimeContext.getTaskName();//获取task name
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        // 关闭这个process
        super.close();
    }
});
```

#### keyedProcessFunction

```java
Configuration configuration = new Configuration();
configuration.setInteger("rest.port",8822);
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");
env.setParallelism(1);

DataStreamSource<String> source1 = env.socketTextStream("node1", 9998);
// 1.keyby分组
KeyedStream<Tuple2<String, String>, String> keyBy = source1.map(s -> {
    String[] split = s.split("\\s+");
    return Tuple2.of(split[0], split[1]);
}).returns(new TypeHint<Tuple2<String, String>>() {}).keyBy(key -> key.f0);


SingleOutputStreamOperator<Tuple2<Integer, String>> process =
    keyBy.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>() {
        //第一个泛型是key的类型，第二个是这条数据的类型，第三个是返回值
        private int count;
        @Override
        public void processElement(Tuple2<String, String> t2, KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>.Context context, Collector<Tuple2<Integer, String>> collector) throws Exception {
            // 这里的数据并没有分组，还是一条一条来的
            count++;
            collector.collect(Tuple2.of(Integer.parseInt(t2.f0), String.valueOf(count)));
        }
    });

process.print();
env.execute();
```



## 算子

### 分区算子

> partition用于指定上游task的各并行subtask和下游task的subtask间如何传输数据
>
> 基类ChannelSelector，流式实现StreamPartitioner

| 类型                         | 描述                           |
| ---------------------------- | ------------------------------ |
| dataStream.global();         | 全部发往第一个                 |
| dataStream.broadcast();      | 广播                           |
| dataStream.forward();        | 上下游并行度一样时，一对一发送 |
| dataStream.shuffle();        | 随机                           |
| dataStream.rebalance();      | Round-Robin，轮询              |
| dataStream.recale();         | Local Round-Robin，本地轮询    |
| dataStream.partitionCustom() | 自定义单播                     |
| dataStream.keyby()           | 根据key的hash值发送            |





### transformation

- project

- fliter

- keyby

- max 和maxBy（相等不替换）

  没有聚合的列也可以返回，但是

  max只更新要求max的字段

  maxBy替换全部的

- min和minBy同上

- **reduce**

  ```java
  reduce需要实现一个接口;
  第一个参数为上一次的聚合结果，
  第二个为当前数据;
  ```


### sink

StreamingFileSink

- 分桶，默认时间分桶

- 可列式

- 精确一次

  当这个算子写出文件时，文件会经历三种状态

  1. in-progress Files：正在写
  2. Pending Files：写满了，挂起，写下一个，出错回滚
  3. finish Files：写完了。数据提交事务，认为是可靠的

  ```java
  public class Demo2 {
      public static void main(String[] args) throws Exception {
  		//  String path = "F:\\JavaProject\\flink-test\\mode1\\src\\outfile\\";
  
          String path = "file:///F:/JavaProject/flink-test/mode1/src/outfile/";
  
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
          env.getCheckpointConfig().setCheckpointStorage(path+"checkpoint");
          env.setParallelism(1);
          DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());
          // StreamingFileSink方法
          // 输出为行格式
          FileSink<String> build = FileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("utf8"))
                  //文件滚动的策略
                  .withRollingPolicy(DefaultRollingPolicy.builder()
                          // 10s滚动一次
                          .withRolloverInterval(10000)
                          // 多少大小滚动
                          .withMaxPartSize(1024 * 1024).build())
                  // 分桶的策略，按时间,DateTimeBucketAssigner<>泛型要求一条数据的格式，写出javabean，则要求该bean
                  .withBucketAssigner(new DateTimeBucketAssigner<String>())
                  // 多久检查一次该桶满了没。是不是要创建下一个桶了
                  .withBucketCheckInterval(5)
                  .withOutputFileConfig(
                          // 输出文件的相关配置，文件名和类型
                          OutputFileConfig.builder()
                                  .withPartPrefix("doitedu")
                                  .withPartSuffix(".txt")
                                  .build())
                  .build();
          DataStream<String> map1 = source.map(JSON::toJSONString);
          //map1.sinkTo(build);
  
  
          // 输出成列格式
          // 根据调用当方法即传入的信息，获取avro模式和schema，生成ParquetWriter。因为parquet自带schema
          // 方法一
          // ParquetAvroWriters.forGenericRecord(new Schema());//自己生成太麻烦，不建议
          // 方法二
          ParquetWriterFactory<EventBean> schema = ParquetAvroWriters.forSpecificRecord(EventBean.class);//需要avro文件，读取反射成自己的schema
          FileSink<EventBean> sink2 = FileSink.forBulkFormat(new Path(path), schema)
                  //文件滚动的策略,只有一种滚动模式，只有发生了checkpoint时才滚动
                  .withRollingPolicy(OnCheckpointRollingPolicy.build())
                  // 分桶的策略，按时间
                  .withBucketAssigner(new DateTimeBucketAssigner<EventBean>())
                  // 多久检查一次该桶满了没。是不是要创建下一个桶了
                  .withBucketCheckInterval(5)
                  .withOutputFileConfig(
                          // 输出文件的相关配置，文件名和类型
                          OutputFileConfig.builder()
                                  .withPartPrefix("doitedu")
                                  .withPartSuffix(".parquet")
                                  .build())
                  .build();
          DataStream<EventBean> map2 = source.map(eventLog -> {
              HashMap<CharSequence, CharSequence> hashMap = new HashMap<>(eventLog.getEventInfo());
              return new EventBean(eventLog.getGuid(), eventLog.getSessionId(),
                      eventLog.getEventId(), eventLog.getTimeStamp(), hashMap);
          }).returns(EventBean.class);
           map2.sinkTo(sink2);
          // 方法三
          // 传入普通的一个javaBean即可
          ParquetWriterFactory<EventLog> factory = ParquetAvroWriters.forReflectRecord(EventLog.class);
          FileSink<EventLog> sink3 = FileSink.forBulkFormat(new Path(path), factory)
                  .withBucketAssigner(new DateTimeBucketAssigner<EventLog>())
                  .withBucketCheckInterval(5)
                  .withRollingPolicy(OnCheckpointRollingPolicy.build())
                  .withOutputFileConfig(
                          OutputFileConfig.builder()
                                  .withPartSuffix(".parquet")
                                  .withPartPrefix("flink").build())
                  .build();
          // source.sinkTo(sink3);
  
          env.execute();
      }
  }
  ```

  

  ```xml
  <!--  StreamingFileSink写出文件的扩展包  -->
  <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-files</artifactId>
      <version>1.14.4</version>
  </dependency>
  <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-parquet_2.12</artifactId>
      <version>1.14.4</version>
  </dependency>
  <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-avro</artifactId>
      <version>1.14.4</version>
  </dependency>
  <dependency>
      <groupId>org.apache.parquet</groupId>
      <artifactId>parquet-avro</artifactId>
      <version>1.12.3</version>
  </dependency>
  <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <version>3.3.0</version>版本可能不兼容
  </dependency>
  </dependencies>
  
  # 构建列式输出，第二种的方法
  <build>
      <plugins>
          <plugin>
              <groupId>org.apache.avro</groupId>
              <artifactId>avro-maven-plugin</artifactId>
              <version>1.10.2</version>
              <executions>
                  <execution>
                      <phase>generate-sources</phase>
                      <goals>
                          <goal>schema</goal>
                      </goals>
                      <configuration>
                          <sourceDirectory>${project.basedir}/src/main/resources/</sourceDirectory>
                          <outputDirectory>${project.basedir}/src/main/java/</outputDirectory>
                      </configuration>
                  </execution>
              </executions>
          </plugin>
          <plugin>
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-compiler-plugin</artifactId>
              <configuration>
                  <source>1.8</source>
                  <target>1.8</target>
              </configuration>
          </plugin>
      </plugins>
  </build>
  
  ```
  
  
  
  
  
  

### source

TextInputFormat

SequenceFileInputFormat

ParquetFileInputFormat

```java
// 带web环境的
Configuration conf = new Configuration();
conf.setInteger("rest.port",8081);
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
env.setParallelism(1); // 修改默认并行度

// source来源算子,相当于spark中的textFile
DataStreamSource<String> source = env.socketTextStream("192.168.88.161",9999);
DataStreamSource<Integer> from = env.fromElements(1,2,3,4,5);
from.map(v->v*10).print();

List<String> dataList = Arrays.asList("a","b","c");
DataStreamSource<String> fromCollection = env.fromCollection(dataList);
fromCollection.map(String::toString).print();

DataStreamSource<Long> sequence = env.generateSequence(1, 100);
sequence.map(v->v+1).print();

/*------------------------上面的都是单并行度-----------------------------*/
// 得到多并行度的
env.fromParallelCollection(new LongValueSequenceIterator(1,100),
                           TypeInformation.of(LongValue.class)).map(v->v.getValue()+1).setParallelism(3).print();


// 读取socket，后俩参数：分割符，重试c
env.socketTextStream("localhost",8080,"\s",2);

// PROCESS_COMTINUOUSLY文件变化，重新读取整个文件重新算，interval读取时间间隔
env.readFile(new TextInputFormat(null),"path",
             FileProcessingMode.PROCESS_CONTINUOUSLY,1);
```

#### kafkaSource

flink作为消费者消费kafka的数据

- 实现SourceFunction比如：FlinkKafkaConsumer

    

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

// KafkaSource 底层封装了kafka的consumer
KafkaSource.<String>builder()
    .setTopics(List.of("test"))
    .setGroupId("test")
    .setBootstrapServers("node1:9092,node2:9092")
    // 读取数据的起始位置
    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))//最新
    //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST) //最早
    //.setStartingOffsets(OffsetsInitializer.offsets(Map.of(new TopicPartition("主题1",1),100L)))
    .setValueOnlyDeserializer(new DeserializationSchema<String>() {
        @Override
        public String deserialize(byte[] bytes) throws IOException {
            // bytes：就是传过来的消息的value，二进制，转字符串返回即可
            return new String(bytes);
        }

        @Override
        public boolean isEndOfStream(String s) {
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    })//反序列化new SimpleStringSchema(),上述反序列化javabean时需要自己写
    // 默认无界流，设置有界流，设置了读到该位置则停止，且退出程序，常用于补数或重跑
    .setBounded(OffsetsInitializer.committedOffsets())
    // 设置成无界流（假无界），但是并不会一直读下去，到达指定位置停止，但是程序不退出
    .setUnbounded(OffsetsInitializer.latest())
    // flink有一个状态管理机制的，它把kafka作为了算子，算子状态记录了读取kafka的topic的分区的偏移量
    // flink优先按自己记录的来读，再按kafka的consumer读取
    .setProperty("auto.offset.commit","true")//设置kafka的自动提交
    .build();
```

#### SourceFunction

```java
public class CustomSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());
        source.map(JSON::toJSONString).print();
        env.execute();

    }
}

class MySourceFunction implements SourceFunction<EventLog> {
    volatile boolean flag = true;
    @Override
    public void run(SourceContext<EventLog> sourceContext) throws Exception {
        EventLog eventLog = new EventLog();
        String[] events = {"appLaunch","pageLoad","addShow","addClick","itemShare","itemCollect","wakeUp","appClose"};
        HashMap<String, String> eventInfoMap = new HashMap<>();
        while (flag){
            eventLog.setGuid(RandomUtils.nextLong(1,1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimeStamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0,events.length)]);
            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1),RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfoMap);
            sourceContext.collect(eventLog);
            eventInfoMap.clear();

            Thread.sleep(RandomUtils.nextInt(500,1500));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}


@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
class EventLog{
    private Long guid;
    private String sessionId;
    private String eventId;
    private long timeStamp;
    private Map<String,String> eventInfo;
}

```





**java流式计算wordcount**

```java
public class Main {
    public static void main(String[] args) throws Exception {
        // 创建一个编程入口环境,本地运行时默认的并行度为逻辑核数·
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // 修改默认并行度
        DataStreamSource<String> source = env.socketTextStream("192.168.88.161",9999);
        DataStream<Tuple2<String,Integer>> words = source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception{
                String[] split = s.split("\\s+");
                for(String word:split){
                    collector.collect(Tuple2.of(word,1));
                }
            }
        }).setParallelism(4); //每个算子可以自己设置并行度

        KeyedStream<Tuple2<String,Integer>,String> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String,Integer>> resultStream = keyed.sum(1);// 或者字段“f1”
        resultStream.print("wcSink");
        words.print("words");

        env.execute();
    }
}
```

**java批计算**

```java
public class BatchWorldCount {
    public static void main(String[] args) throws Exception {
        // 创建环境
        ExecutionEnvironment batchenv = ExecutionEnvironment.getExecutionEnvironment();

        // 读数据
        DataSource<String> dataSource = batchenv.readTextFile("F:\\JavaProject\\flink-test\\mode1\\words");
        dataSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split("\\s+");
                for (String word : words) {
                    if (word.length()>0)
                        collector.collect(Tuple2.of(word,1));
                }
            }
        }).groupBy(0).sum(1).print();//或者f0
    }
}

```

流批一体

```java
public static void main(String[] args) throws Exception {
    // 创建一个编程入口

    // 流式处理入口环境
    StreamExecutionEnvironment envStream = StreamExecutionEnvironment.getExecutionEnvironment();
    envStream.setParallelism(1);
    envStream.setRuntimeMode(RuntimeExecutionMode.BATCH);
    // 读文件 得到datestream
    DataStreamSource<String> streamSource = envStream.readTextFile("F:\\JavaProject\\flink-test\\mode1\\words");

    // 调用dataStream的算子计算
    streamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split("\\s+");
            for (String word : words) {
                if (word.length()>0)
                    collector.collect(Tuple2.of(word,1));
            }
        }
    }).keyBy(
            new KeySelector<Tuple2<String, Integer>, String>() {
                @Override
                public String getKey(Tuple2<String, Integer> value) throws Exception {
                    return value.f0;
                }
            }
    ).sum(1).print();

    envStream.execute();
}
```



## 多流操作

### 侧流输出

```java
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
                    // TypeInformation.of(EventLog.class)缺少会导致类型推断不出来，分走了读取不出来，但是不报错
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

```

### connect

两个流的数据可以不一致，分开处理，但是使用map时，最后需要返回成一个相同类型的

```java
public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    configuration.setInteger("rest.port",8822);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");
    env.setParallelism(1);

    DataStreamSource<String> source1 = env.socketTextStream("node1", 9999);
    DataStreamSource<String> source2 = env.socketTextStream("node1", 9998);
    ConnectedStreams<String, String> connect = source1.connect(source2);
    SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, String, String>() {
        // map完成后会返回成一个流
        final String prefix = "Pre_";

        @Override
        public String map1(String s) throws Exception {
            //第一个流map逻辑，假设全是数字，乘10加前缀
            return prefix + (Integer.parseInt(s) * 10);
        }

        @Override
        public String map2(String s) throws Exception {
            //第二个流，全部转大写，加前缀
            return prefix + s.toUpperCase();
        }
    });
    map.print();
    env.execute();
}
```





### union

需要两个数据类型一致的流

```java
public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    configuration.setInteger("rest.port",8822);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointStorage("file:///F:/JavaProject/flink-test/mode1/src/outfile/checkpoint");
    env.setParallelism(1);

    DataStreamSource<String> source1 = env.socketTextStream("node1", 9999);
    DataStreamSource<String> source2 = env.socketTextStream("node1", 9998);
    DataStream<String> union = source1.union(source2);
    union.print();
    env.execute();
}
```

### 协同分组

```java
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

    CoGroupedStreams<Tuple2<String, String>, Tuple3<String, String, String>> coGroup =  .coGroup(stream2);
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
```

### join

协同分组的实现

```java
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
```



### BroadCast

> 一个流连接广播流，实现process方法，将会提供两个方法
>
> processElement
>
> processBroadcastElement

广播

```java
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
```

## 时间语义

watermark:水印 

由于业务数据产生到计算之间有一定的延迟，不同的时间依据产生不同的结果，需要定义按什么时间处理.

一般影响窗口计算和定时器

- `process time` 处理时间，该条数据被flink计算的时间

- `event time` 事件时间，业务发生时的时间，一般数据中有记录

    > 由于各种问题，事件时间可能停止，还可能出现迟到的数据。但是时间单调递增不可回退

- `Ingestion time` 注入时间，source读取时间

**设计意义**：统一API

![](C:\Users\49584\Desktop\Java\picture\flink时间的推进.png)

### 生成策略

**周期生成**

```java
AssignerWithPeriodicWatermarks
```



**新版本**

- 紧跟最大时间的watermark生成策略

    > 完全不允许乱序
    >
    > ```java
    > WatermarkStrategy.forMonotonousTimestamp();
    > ```

- 允许乱序的watermark生成策略

    >如果按照事实最大时间推进时间，将会造成大量过期数据，如何缓解过期数据
    >
    >- 1. 容错时间，即 最大乱序时间，根据最大时间减去一个容错时间
    >
    >```java
    >WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10));
    >```

- 自定义watermark生成策略

    > ```java
    > WatermarkStrategy.forGenerator(new WatermarkGenerator(){});
    > ```

    

### 源头水印生成

![](C:\Users\49584\Desktop\Java\picture\watermark调用流程.png)

### 下游算子更新水印

![](C:\Users\49584\Desktop\Java\picture\watermark下游算子.png)

**代码**

```java
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

stream.process(new ProcessFunction<String, String>() {
    @Override
    public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
        context.timestamp();// 该条数据中记录的时间
        context.timerService().currentWatermark();//此刻watermark
        context.timerService().currentProcessingTime();// 此刻process的处理时间
        collector.collect(s);
    }
});

stream.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator();
```

## 窗口

### 分类

![](C:\Users\49584\Desktop\Java\picture\window分类.png)



### 算子

![](C:\Users\49584\Desktop\Java\picture\window算子.png)

#### trigger

> 如何触发该窗口的计算

#### evictor

> 窗口计算完了怎么移除数据，需不需要保留一部分

#### allowedLateness

> 数据迟到了怎么处理的，要是桶没被清理掉。可以再加进去

#### sideOutputLateDate

> 超级晚到的数据输出到侧流

- 

### 窗口指派

> 当调用stream.window()方法时,指派开窗类型

#### NonKeyed

```java
// 处理时间语义，滚动窗口
source.windowAll(TumblingProcssingTimeWindows.of(Time.seconds(5)));
// 处理时间语义，滑动窗口
source.windowAll(SlidingProcssingTimeWindows.of(Time.seconds(5),Time.seconds(5)));

// 事件时间语义，滚动窗口
source.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));
// 事件时间语义，滑动窗口
source.windowAll(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(5)));

source.countWindowAll(100);
source.countWindwAll(100,20)
```

#### **Keyed**

```java
KeyedStreay keyed =source.keyBy();
// 处理时间语义，滚动窗口
keyed.window(TumblingProcssingTimeWindows.of(Time.seconds(5)));
// 处理时间语义，滑动窗口
keyed.window(SlidingProcssingTimeWindows.of(Time.seconds(5),Time.seconds(5)));

// 事件时间语义，滚动窗口
keyed.window(TumblingEventTimeWindows.of(Time.seconds(5)));
// 事件时间语义，滑动窗口
keyed.window(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(5)));

keyed.countWindow(100);
keyed.countWindw(100,20);

```

#### 聚合窗口

**window后的聚合方式**

![](C:\Users\49584\Desktop\Java\picture\apply_agg.png)

- apply和aggregate是基础的聚合方式
- **apply**：当触发时，将整个数据一起给
- **aggregate**：来一条给一条
- **process**：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
DataStreamSource<String> source = env.socketTextStream("up01", 9999);
SingleOutputStreamOperator<EventBean> beanStream = source.map(s -> {
    String[] split = s.split("\\s+");
    return new EventBean(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3]);
}).returns(EventBean.class);

SingleOutputStreamOperator<EventBean> watermarks = beanStream.assignTimestampsAndWatermarks(
        WatermarkStrategy.<EventBean>forBoundedOutOfOrderness(Duration.ofMillis(0))
        .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
            @Override
            public long extractTimestamp(EventBean eventBean, long l) {
                return eventBean.getDataTime();
            }
        })
);
watermarks.print();



SingleOutputStreamOperator<Double> result = watermarks.keyBy(EventBean::getId)
        // 按事件事件的滑动窗口,第一个time为窗口长度，参数2滑动步长，每滑动一次触发一次输出
        .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
        .aggregate(new AggregateFunction<EventBean, Tuple2<Integer, Integer>, Double>() {
            // 第一个是数据类型，第二个是累加器的数据类型，第三个是返回的结果类型
            @Override
            public Tuple2<Integer, Integer> createAccumulator() {
                //初始化累加器
                return Tuple2.of(0, 0);
            }

            @Override
            public Tuple2<Integer, Integer> add(EventBean eventBean, Tuple2<Integer, Integer> accumulator) {
                accumulator.setField(accumulator.f0 + 1, 0);//记录数据条数
                accumulator.setField(accumulator.f1 + eventBean.getNum(), 1);// 汇总num
                //或者直接new 一个新的
                // Tuple2<Integer, Long> tuple2 = Tuple2.of(accumulator.f0 + 1, accumulator.f1 + eventBean.getNum());
                return accumulator;
            }

            @Override
            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                return accumulator.f1 / (double) accumulator.f0;
            }

            @Override
            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> currAcc, Tuple2<Integer, Integer> acc1) {
                /*
                 * 在批计算中，shuffle的上游可以做局部聚合，然后吧局部聚和的结果交给下游做全局聚合
                 * 因此，需要提供两个局部聚合的合并逻辑
                 *
                 * 在流式计算中，不存在聚合机制，该方法不会用到.
                 * 若需要流批一体，可以实现
                 * */
                return Tuple2.of(currAcc.f0 + acc1.f0, currAcc.f1 + acc1.f1);
            }
        }).setParallelism(1); //这里并行度要设为1，否则发往不同分区了，看不到结果
result.print();




//需求2：每隔10s统计最近30s的数据中，行为事件最长的记录
watermarks.keyBy(EventBean::getId).window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
                //泛型1：数据类型。泛型2：输出结果类型，泛型3：key的类型，泛型4：窗口类型
                .apply(new WindowFunction<EventBean, EventBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<EventBean> iterable, Collector<EventBean> collector) throws Exception {
                        /**
                          @param s 本次传给窗口的key
                         * @param timeWindow 窗口的元信息，比如窗口的起始时间和结束时间
                         * @param iterable 窗口里面的所有数据的迭代器
                         * @param collector 收集结果
                         * @return void
                         */
                        // 建议自己迭代，这样占内存
                        ArrayList<EventBean> beans = new ArrayList<>();
                        for (EventBean bean : iterable) {
                            beans.add(bean);
                        }
                        Collections.sort(beans, new Comparator<EventBean>() {
                            @Override
                            public int compare(EventBean o1, EventBean o2) {
                                return (int)(o2.getDataTime() - o1.getDataTime());
                            }
                        });
                        for (int i = 0; i < Math.min(beans.size(), 2); i++) {
                            collector.collect(beans.get(i));
                        }
                    }
                });

// process, 包含apply所有信息，桶apply,窗口触发时,所有数据一起给到
watermarks.keyBy(EventBean::getDataTime).window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
                .process(new ProcessWindowFunction<EventBean, String, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<EventBean, String, Long, TimeWindow>.Context context,
                                        Iterable<EventBean> iterable, Collector<String> collector) throws Exception {

                        TimeWindow window = context.window();
                        //若本窗口范围,[1000,2000) ,
                        window.maxTimestamp(); // 1999
                        window.getStart();// 1000
                        window.getEnd();//2000
                        // 建议自己迭代，这样占内存
                        ArrayList<EventBean> beans = new ArrayList<>();
                        for (EventBean bean : iterable) {
                            beans.add(bean);
                        }
                        Collections.sort(beans, new Comparator<EventBean>() {
                            @Override
                            public int compare(EventBean o1, EventBean o2) {
                                return (int)(o2.getDataTime() - o1.getDataTime());
                            }
                        });

                        // 带上窗口信息
                        for (int i = 0; i < Math.min(beans.size(), 2); i++) {
                            collector.collect(JSON.toJSONString(beans.get(i))+","+window.getStart()+","+window.getEnd());
                        }
                    }
                });

env.execute();
```

### 开窗API

```java
/**
 * 各种开窗API
 * 全局 计数滑动窗口
 * 全局 计数滚动窗口
 * 全局 事件时间滑动窗口
 * 全局 事件时间滚动窗口
 * 全局 事件时间会话窗口
 * 全局 处理时间滑动窗口
 * 全局 处理时间滚动窗口
 * 全局 处理时间会话窗口
 *
 *  keyed 计数滑动窗口
 *  keyed 计数滚动窗口
 *  keyed 事件时间滑动窗口
 *  keyed 事件时间滚动窗口
 *  keyed 事件时间会话窗口
 *  keyed 处理时间滑动窗口
 *  keyed 处理时间滚动窗口
 *  keyed 处理时间会话窗口
 */

public class window_demo2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("up01", 9999);
        SingleOutputStreamOperator<EventBean> beanStream = source.map(s -> {
            String[] split = s.split("\\s+");
            return new EventBean(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3]);
        }).returns(EventBean.class);

        SingleOutputStreamOperator<EventBean> watermarks = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<EventBean>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                            @Override
                            public long extractTimestamp(EventBean eventBean, long l) {
                                return eventBean.getDataTime();
                            }
                        })
        );
        watermarks.print();


        // 全局 计数滑动窗口
        beanStream.countWindowAll(10)// 窗口长度10条数据
                .apply(new AllWindowFunction<EventBean, String, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow globalWindow, Iterable<EventBean> iterable, Collector<String> collector) throws Exception {

            }
        });
        // 全局 计数滚动窗口
        beanStream.countWindowAll(10,2);//窗口长度10条数据，滑动步长为2条数据
                /*.apply()*/

        // 全局 事件时间滑动窗口
        watermarks.windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                .apply(new AllWindowFunction<EventBean, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<EventBean> iterable, Collector<String> collector) throws Exception {

                    }
                });
        // 全局 事件时间滚动窗口
        watermarks.windowAll(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)));
                /*.apply()*/
        // 全局 事件时间会话窗口
        watermarks.windowAll(EventTimeSessionWindows.withGap(Time.seconds(30))); //前后两条数据超过30s划分窗口
        // 全局 处理时间滑动窗口
        beanStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30),Time.seconds(10)));
        // 全局 处理时间滚动窗口
        beanStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)));
        // 全局 处理时间会话窗口
        beanStream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));


        KeyedStream<EventBean, String> keyBy = watermarks.keyBy(EventBean::getId);
        //  keyed 计数滑动窗口
        keyBy.countWindow(10);
        //  keyed 计数滚动窗口
        keyBy.countWindow(10,2);
        //  keyed 事件时间滑动窗口
        keyBy.window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)));
        //  keyed 事件时间滚动窗口
        keyBy.window(TumblingEventTimeWindows.of(Time.seconds(30)));
        //  keyed 事件时间会话窗口
        keyBy.window(EventTimeSessionWindows.withGap(Time.seconds(30)));
        //  keyed 处理时间滑动窗口
        keyBy.window(SlidingProcessingTimeWindows.of(Time.seconds(30),Time.seconds(10)));
        //  keyed 处理时间滚动窗口
        keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(30)));
        //  keyed 处理时间会话窗口
        keyBy.window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));

    }
```



### 数据延迟处理

> 小延迟（乱序）：watermark容错，减慢时间的推进，让本已经迟到的数据认为并没有迟到
>
> 中延迟（乱序）：allowedLateness，允许一定限度的迟到，并对迟到的数据重新触发窗口的运算
>
> 大延迟（乱序）：用sideOutputData，输出到侧流中，直接到达最终结果，并合并

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
DataStreamSource<String> source = env.socketTextStream("up01", 9999);
SingleOutputStreamOperator<EventBean> beanStream = source.map(s -> {
    String[] split = s.split("\\s+");
    return new EventBean(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3]);
}).returns(EventBean.class);

SingleOutputStreamOperator<EventBean> watermarks = beanStream.assignTimestampsAndWatermarks(
        WatermarkStrategy.<EventBean>forBoundedOutOfOrderness(Duration.ofMillis(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                    @Override
                    public long extractTimestamp(EventBean eventBean, long l) {
                        return eventBean.getDataTime();
                    }
                })
);

OutputTag<EventBean> outputTag = new OutputTag<EventBean>("侧流字符串标识", TypeInformation.of(new TypeHint<EventBean>() {}));
SingleOutputStreamOperator<Tuple2<String, Integer>> result = watermarks.keyBy(EventBean::getId)
        .window(TumblingEventTimeWindows.of(Time.seconds(30)))
        .allowedLateness(Time.seconds(2))//允许迟到2s
        .sideOutputLateData(outputTag) // 超过允许迟到后，输出到侧流
        .apply(new WindowFunction<EventBean, Tuple2<String, Integer>, String, TimeWindow>() {

            @Override
            public void apply(String s, TimeWindow timeWindow, Iterable<EventBean> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Tuple2<String, Integer> tuple2 = Tuple2.of(s+" "+timeWindow.getStart()+" "+timeWindow.getEnd(), 0);
                for (EventBean bean : iterable) {
                    tuple2.f1 = tuple2.f1 + 1;
                }
                collector.collect(tuple2);
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {});

result.getSideOutput(outputTag).print("侧流 -> ");
result.print("keyed窗口数据条数 ->");

env.execute();
```

## 状态

checkpoint

> `ValueState`：只能存一个
>
> `ListState`：列表，重启后subtask轮询平均分配ListState
>
> `UnionState`: 广播，重启后的每个subtask，都能拿到checkpoint的完整State
>
> `MapState`：字典
>
> `aggregatingState`: 需要两个泛型，第一个泛型代表数据，第二条为输出
>
> `reducingState`: 一个泛型，要求输入输出类型一致

### 算子状态

> 1. 算子状态，是每个subtask自己持有一份独立的状态数据
>     - 若失败重新恢复后，算子并行度发生变化，则转台将在新的subtask之间均匀分配
> 2. 算子函数实现后（checkpointFunction），即可使用算子状态。
> 3. 算子状态通常用于source中，其他情况下建议用keyedState

```java
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
```

### 键控状态

```java
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
source.keyBy(s->s)
    .map(new RichMapFunction<String, String>() {
        ListState<String> listState;
        MapState<String,String> mapState;
        ReducingState<String> reducingState;
        AggregatingState<Integer,Double> aggregatingState;//泛型1：进来的数据，泛型2，输出的数据

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            RuntimeContext context = getRuntimeContext();
            // 获取一个list结构的状态存储器
            listState = context.getListState(new ListStateDescriptor<String>("name", String.class));
            reducingState =context.getReducingState(new ReducingStateDescriptor<String>("reduce", new ReduceFunction<String>() {
                @Override
                public String reduce(String s, String t1) throws Exception {
                    /**
                                 * @param s 传进来的数据
                                 * @param t1 reduce的结果
                                 * @return java.lang.String
                                 **/
                    return s+""+t1;
                }
            },String.class));

            aggregatingState = context.getAggregatingState(
                new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>("agg", 
                                                                                         new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                                                                             /**
                                     * 泛型1：进来的数据
                                     * 泛型2：累加器
                                     * 泛型3：输出的类型Ouble
                                     **/
                                                                                             @Override
                                                                                             public Tuple2<Integer, Integer> createAccumulator() {
                                                                                                 return Tuple2.of(0,0);
                                                                                             }

                                                                                             @Override
                                                                                             public Tuple2<Integer, Integer> add(Integer integer, Tuple2<Integer, Integer> acc) {
                                                                                                 return Tuple2.of(acc.f0+1,acc.f1+integer);
                                                                                             }

                                                                                             @Override
                                                                                             public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                                                                 return accumulator.f1/(double)accumulator.f0;
                                                                                             }

                                                                                             @Override
                                                                                             public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> acc0, Tuple2<Integer, Integer> acc1) {
                                                                                                 return Tuple2.of(acc1.f0+acc0.f0,acc0.f1+acc1.f1);
                                                                                             }
                                                                                         }, TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                                                                                         })));
            // 获取一个map结构的状态存储器
            // 获取一个单值结构的状态存储器

        }
        @Override
        public String map(String s) throws Exception {
            ZonedDateTime now = Instant.now().atZone(ZoneId.of("Asia/Shanghai"));// 获取当前时间带时区
            DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            //System.out.println(dtf1.format(now));// 2022-10-03 21:00:45
            listState.add(s);
            // listState.update(Arrays.asList("a","b"));先清空再添加
            StringBuilder builder = new StringBuilder();
            for (String s1 : listState.get()) {
                builder.append(s1);
            }

            //mapState的操作
            mapState.contains("a");
            mapState.get("a");
            mapState.put("a","value");
            Iterator<Map.Entry<String, String>> iterator = mapState.iterator();
            Iterable<Map.Entry<String, String>> entryIterable = mapState.entries();

            //reduceState,里面只保存了一个值
            reducingState.add("a");
            reducingState.get();


            // agg
            aggregatingState.add(10);
            aggregatingState.get();//10.0
            return dtf1.format(now)+" "+ builder.toString();
        }
    }).print();

env.execute();
```

### TTL管理

> - flink可以对状态数据进行存货时长管理
>
> - 淘汰机制基于存活时间
>
> - 存活时间的计算可以在数据读写时刷新 

#### 更新策略

```
.updateTtlOnCreateAndWrite() // 当插入和更新的时候,刷新ttl计时
.updateTtlOnReadAndWrite()   // 读写时都会刷新ttl计时
.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//和上面的返回策略一样
```

#### 可见性

- 过期数据不返回
- 返回过期数据

    // 设置状态的可见性
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)// 永不返回已过期的
    //如果还没有清除，返回过期的数据
    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)

#### 语义选择

将ttl设置处理时间语义,或者.useProcessingTime(),默认的时间就是processingTime

```
.setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
//.useProcessingTime();
```

#### 清理策略

> 1. 增量清理
>
>     当访问状态时，会驱动一次检查，如状态数据很大，则检查一部分。针对本地状态空间且用于HashMapStateBackend
>
>     .cleanupIncrementally(3,true)//增量清理策略，第一个参数表示最多一次迭代多少个key的state，不建议true
>
> 2. 全量快照
>
>     checkpoint时，将算子本地的state中未过期数据写入checkpoint的快照中，本地的数据不会删除，只针对checkpoint
>
> 3. 状态后端
>
>     状态的两种存放方式
>
>     - HashMapStateBackend：heap堆，放内存中，可能溢出到磁盘，`默认`
>
>     - RocksDbStateBackend：内嵌数据库，以序列化的kv字节存储
>
>         用该模式需要引入配置
>
>         ```xml
>         <dependency>
>             <groupId>org.apache.flink</groupId>
>             <artifactId>flink-statebackend-rocksdb_2.12</artifactId>
>             <version>1.14.4</version>
>         </dependency>
>         ```
>
>         ```java
>         // 设置两种状态后端
>         env.setStateBackend(new HashMapStateBackend());
>         EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
>         env.setStateBackend(backend);
>         ```
>
> `超大state一般用rockedDb,两种状态后端在生成checkpoint快照时，文件格式一样，重启时可以更改状态后端`

```java
.cleanupIncrementally(3,true)//增量清理策略，第一个
.cleanupFullSnapshot()//全量快照清理，checkpoint时保存未过期的，但是不会清理本地数据
.cleanupInRocksdbCompactFilter(1000)//状态后端
```



#### 代码

```java
DataStreamSource<String> source = env.socketTextStream("192.168.88.163", 9999);
source.keyBy(s->"0").map(new RichMapFunction<String, String>() {
    // 1.声明一个状态
    ValueState<String> valueState;
    ListState<String> listState;
    @Override
    public String map(String s) throws Exception {
        listState.add(s);
        Iterable<String> iterable = listState.get();

        StringBuilder sb = new StringBuilder();
        for (String s1 : iterable) {
            sb.append(s1);
        }
        return sb.toString();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("valueState", String.class);
        ListStateDescriptor<String> descriptor1 = new ListStateDescriptor<>("listDescriptor", String.class);

        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.milliseconds(5000))
            .setTtl(Time.milliseconds(4000)) //和上面的参数一样，冗余了
            .updateTtlOnCreateAndWrite() // 当插入和更新的时候,刷新ttl计时
            .updateTtlOnReadAndWrite()   // 读写时都会刷新ttl计时
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//和上面的返回策略一样
            // 设置状态的可见性
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)// 永不返回已过期的
            //如果还没有清除，返回过期的数据
            .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
            //将ttl设置处理时间语义,或者.useProcessingTime(),默认的时间就是processingTime
            .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
            .useProcessingTime();
            .cleanupIncrementally(3,true)//增量清理策略,默认配置只有这一个，然后参数为5，false，不建议true
            .cleanupFullSnapshot()//全量快照清理，checkpoint时保存未过期的，但是不会清理本地数据
            .cleanupInRocksdbCompactFilter(1000)//状态后端
            .disableCleanupInBackground()
            .build();

        descriptor.enableTimeToLive(ttlConfig); //开启TTL管理
        descriptor1.enableTimeToLive(ttlConfig);
        valueState = getRuntimeContext().getState(descriptor);
        listState = getRuntimeContext().getListState(descriptor1);
    }

}).setParallelism(1).print();
```



## EOS保证

>  flink如何保证exactly-once的

### source端

### checkpoint

### sink端

1. 采用幂等写入方式
2. 两阶段提交写入方式，2PC
3. 采用预写日志2pc提交方式

# Flink-SQL

## SQL执行流程

> 1. 将数据流，绑定源数据（schema），组成catalog的表（table或view）
> 2. 然后由用户通过table API 或 SQL API来表达啊计算逻辑
> 3. 由table-planner利用calcite进行sql语义解析，绑定元数据得到逻辑执行计划
> 4. 再由Optimizer（ROB，CBO）进行优化后，得到物理执行计划
> 5. 物理执行计划经过代码生成器生成代码，得到Transformation Tree
> 6. Transformation Tree转成JobGraph后提交到flink集群执行
>
> - **RBO**：基于规则的优化器
>     1. 分区裁剪，列裁剪
>     2. 谓词下推，投影下推，聚合下推，limit下推，sort下推
>     3. 常量折叠
>     4. 子查询内联转join
> - **CBO**：基于成本的优化



## 创建环境

```java
// 方式1
EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
TableEnvironment env = TableEnvironment.create(settings);

// 方式2
StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment env1 = StreamTableEnvironment.create(environment);
```

## 表级与类

**catalog name**:default_catalog

> 元数据空间

database name:default_database

table name

表：物理上的表，存放在内存种，需要带关键字with

视图：对一段计算逻辑的封装



永久表：可以存放到各种数据源的表中去，下一次启动时可以直接查询,也可给flink集群查询，但是 默认存在flink内存，和临时表一致

临时表：带关键字temporary，只在本次env（session）中有效,存放在一个特定的临时map（temporaryTables）中，不属于任何catalog，若和永久表相同，先查临时表

```java
StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
DataStreamSource<String> source = environment.fromElements("a");
environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
StreamTableEnvironment env = StreamTableEnvironment.create(environment);


env.executeSql("create view view_name as select * from t");
env.executeSql("create temporary view_name as select * from t");

env.executeSql("create table_name (id int ,...) with ('connector'='')");
env.executeSql("create temporary table_name (id int ,...) with ('connector'='')");

// 从一个已存在的表名创建表
Table table = env.from("table_name");
        env.createTable("t_name", TableDescriptor
                .forConnector("connector_name").
                format("csv")
                .schema(Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .build())
                .build());
env.createTemporaryView("t_name",env.fromDataStream(source));
env.createTemporaryView("t_name",table);


// changelogStream的表和流转换
DataStream<Row> changelogStream = env.toChangelogStream(table);
env.fromChangelogStream(changelogStream);
```

**DataStream通过JavaBean转Table**

```java
StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
DataStreamSource<String> source = environment.fromElements("{\"id\":2,\"dataTime\":10212154,\"num\":26,\"msg\":\"male\"}");
environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
StreamTableEnvironment env = StreamTableEnvironment.create(environment);
SingleOutputStreamOperator<EventBean> map = source.map(s -> JSON.parseObject(s, EventBean.class));

map.print();

/*Table table = env.fromDataStream(map,
            Schema.newBuilder()
                    .column("id",DataTypes.STRING())
                    .column("dataTime",DataTypes.BIGINT())
                    .column("num",DataTypes.INT())
                    .column("msg",DataTypes.STRING())
                    .build());*/
Table table = env.fromDataStream(map);
table.execute().print();


Table table1 = env.fromValues(
    DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT()),
        DataTypes.FIELD("dataTime", DataTypes.INT()),
        DataTypes.FIELD("num", DataTypes.INT()),
        DataTypes.FIELD("msg", DataTypes.INT())
    ),
    Row.of("001",10000,25,"message1"),
    Row.of("002",14700,23,"message2"),
    Row.of("003",10200,31,"message3")
);

table1.printSchema();
table1.execute().print();

// 2. 带表名的创建
/*env.createTable("table_name",
                TableDescriptor.forConnector("kafka")
                        .format("csv")
                        .option("","")
                        .schema(Schema.newBuilder().build())
                        .build());*/

// 2.1 从dataStream创建带表名,也可以填入schema
env.createTemporaryView("view_name",map);

// javaBean
@NoArgsConstructor
@AllArgsConstructor
public static class EventBean {
    public String id;
    public long dataTime;
    public int num;
    public String msg;
}
```

### SQL语法

#### 建表

**所有的列名，正规写法都需要加飘号**

> 1. 表达式列
>
> 2. 元数据列
>
> 3. 主键约束，json目前不支持
>
> 4. 
>
>     ```SQL
>     create table tableName(
>         -- id int primary key not enforced， -- 单个主键,主键约束
>     	num int,
>         num2 as num + 10,	-- 表达式列，类型根据字段自动推断，支持函数
>         `offset` BIGINT  METADATA VIRTUAL， -- 和下面两种获取元字段，必须加飘号
>         ts TIMESTAMP_LTZ(3) metadata from 'timestamp',  --从元数据中获取字段,key名必须带引号，同名加飘号
>                         
>         -- primary key (id,num) not enforced --联合主键，主键约束
>     )with（
>     	'connector'='kafka'
>     ）
>     ```
>
>     **jsonFormat**
>
>     
>
>     ```sql
>     //假设数据长这样，下面为对应的SQL
>     {"id":1,"friends":[{"name":"a","info":{"address":"北京","gender":"male"}},{"name":"b","info":{"address":"深圳","gender":"female"}}]}
>                     
>     create table test_json3(                                           
>             id int,                                                        
>             friends array<row<name string,info mape<string,string>>>       
>     )with(                                                             
>             'connector' = 'filesystem',                                    
>             'path' = 'F:/JavaProject/flink-test/TestData/',                  
>             'format' = 'json'                                              
>         )；
>                         
>      -- 这里数组下标从1开始，hive中从0开始
>      select id,
>      		friends[1].name as name,
>      		friends[1].info["address"] as addr,
>      		friends[1].info["gender"] as gender 
>      from test_json3
>       select id,
>      		friends[1]['name'] as name,
>      		friends[1]['info']["address"] as addr,
>      		friends[1]['info']["gender"] as gender 
>      from test_json3
>      
>      
>     ```
>                
>     **csvFormat**
>                
>     "1",""
>                
>     ```
>     format = csv
>     csv.field-delimiter=','
>     csv.disable-quote-character=false
>     csv.quote-character='"'		// 若数据每个字段值是由 ""包围起来的，则该参数去掉引号，需要上一个参数置为false
>     csv.allow-comments=false    // csv中注释是用 #，这个参数去掉注释 
>     csv.null-literal="aa"  		// "aa" 字符串转化为 null 值。
>     ```



##### 时间语义

> 在表中，处理时间和时间时间可以同时declare
>
> watermark仅代事件时间（rowtime），
>
> - 前几条数据的watermark是null的原因：
>
>     flink一次读取前几条时，定时器还没有启动（默认200ms更新一次）

```sql
create table t_event(
	guid int,
	eventId string,
	eventTime timestamp(3),
	pageId string,
    pt as proctime(), -- 处理时间
	watermark for eventTime as eventTime - interval '1' second -- 时间时间
)WITH(
            'connector' = 'kafka',
            'topic' = 'json',
            'properties.bootstrap.servers' = 'node3:9092',
            'properties.group.id' = 'testGroup',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
      )
```

##### kafka-connector

> 产生的数据以及能接收的数据流只能是append-only（+I）

**headers中的值，解析后是字节数组，需要cast转**

![](C:\Users\49584\Desktop\Java\picture\kafka-connector-test-data.png)

```sql
create table kafkaTable(                                          
    `meta_time` timestamp(3) metadata from 'timestamp',           
    `partition` bigint metadata virtual,                          
    `offset` 	bigint metadata virtual,                          
    `inKey_k1`  bigint,                                           
    `inKey_k2`  string,                                           
    `guid` 		bigint,                                           
    `eventId`   string,                                           
    `k1`		bigint,                                           
    `k2`		string,                                           
    `eventTime` bigint,                                           
    `headers`	map<string,BYTES> metadata FROM 'headers'   -- 类型固定了      
)with(                                                            
    'connector' = 'kafka',                                        
    'topic' = 'test_kafka',                                       
    'properties.bootstrap.servers' = 'node3:9092',                
    'properties.group.id' = 'testGroup',                          
    'scan.startup.mode' = 'earliest-offset',                      
    'key.format' = 'json',                                        
    'key.json.fail-on-missing-field' = 'false',                   
    'key.json.ignore-parse-errors' = 'true' ,                     
    'key.fields'='inKey_k1;inKey_k2',                -- 建表时，根据kafka key创建的字段             
    'key.fields-prefix'='inKey_',                    -- 在查询字段去掉该前缀后去查找
    'value.format' = 'json',                                      
    'value.json.fail-on-missing-field' = 'false',                 
    'value.fields-include' = 'EXCEPT_KEY'             -- 开启key的单独解析            
)                                                          

--  获取headers中的数据
"select guid,eventId,eventTime,cast(headers['head2'] as string) as headers  from kafkaTable"
```

##### upsert-kafka



## 流转表

流转表时，默认是不传递watermark，需要指定schema，用元数据+source_watermark

或者也可以自己根据字段值重新创建

```java
// 2.带schema,相当于重新定义
tenv.createTemporaryView("streamTable2",watermarks,
                         Schema.newBuilder()
                         .column("guid", DataTypes.INT())
                         .column("eventId", DataTypes.STRING())
                         .column("eventTime", DataTypes.BIGINT())
                         .column("pageId", DataTypes.STRING())
                         .columnByExpression("rt","TO_TIMESTAMP_LTZ(eventTime,3)")
                         .watermark("rt","rt - interval '1' second")
                         .build());
//  3.1 沿用流中watermark方法1，元数据获取,也相当于重新创建
tenv.createTemporaryView("streamTable3",watermarks,
                         Schema.newBuilder()
                         .column("guid", DataTypes.INT())
                         .column("eventId", DataTypes.STRING())
                         .column("eventTime", DataTypes.BIGINT())
                         .column("pageId", DataTypes.STRING())
                         .columnByMetadata("rt",DataTypes.TIMESTAMP_LTZ(3),"rowtime")
                         .watermark("rt","rt - interval '1' second")
                         .build());
//  3.1 沿用流中watermark，底层方法+元数据获取，继承流上的watermark
tenv.createTemporaryView("streamTable4",watermarks,
                         Schema.newBuilder()
                         .column("guid", DataTypes.INT())
                         .column("eventId", DataTypes.STRING())
                         .column("eventTime", DataTypes.BIGINT())
                         .column("pageId", DataTypes.STRING())
                         .columnByMetadata("rt",DataTypes.TIMESTAMP_LTZ(3),"rowtime")
                         .watermark("rt","source_watermark()")
                         .build());
```



## CDC

> 开启bin—log
>
> **mysql**查看binlog的操作
>
> show master status：当前写的binlog
>
> show binary logs：所有binlog
>
> show binlog events in 'mysql-bin.000001'



## 高阶聚合

``` sql
group by cube(省,市,区)
group by grouping sets((省),(省,市),(省,市,区))
group by rollup(省,市,区)
```

## 窗口类型

tumble滚动，

hop滑动，

cumulate累计

> 1.再窗口上做分组聚合，必须带上window_start 和window_end作为分组key
>
> 2.在窗口上做topN计算，必须带上window_start 和window_end作为partition的key
>
> 3.带条件的join，必须包含两个了的window_start和window_end等值条件

窗口JOIN

temporal join

左表去找右边中最新的数据



## 自定义函数

### ScalarFunction

**标量函数**：一进一出



TableFunction

类似于hive中的explode函数，生成新的表

AggregateFunction

```java
```



# 指标监控

## 架构

![](C:\Users\49584\Desktop\Java\picture\metrics收集架构图.png)

## 统计器

### counter

计数器

### gauge

折线图

### histogram



### meter



**实现了前两个**

```java
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
```





# Maven配置

## DataStream

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>mode1</artifactId>
    <version>1.0-SNAPSHOT</version>
    
	<!-- flink相关三个：客户端,java,streaming -->
    <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>1.14.4</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_2.12</artifactId>
            <version>1.14.4</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_2.12</artifactId>
            <version>1.14.4</version>
        </dependency>
        <!-- flink运行时打开webUI -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime-web_2.12</artifactId>
            <version>1.14.4</version>
        </dependency>
        
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-nop</artifactId>
            <version>1.7.2</version>
        </dependency>




        <!-- 读取kafka查询 -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka_2.12</artifactId>
            <version>1.14.4</version>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>2.4.1</version>
        </dependency>

        
        <!--   @生成javaBean     -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>1.18.8</version>
        </dependency>
        <!--   javabean 转json     -->
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.79</version>
        </dependency>


    </dependencies>


</project>
```

## sql

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>flink-table</artifactId>
    <version>1.0-SNAPSHOT</version>

    <!--<properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
    </properties>-->
    <dependencies>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner_2.11</artifactId>
            <version>1.14.4</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-json</artifactId>
            <version>1.14.4</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_2.12</artifactId>
            <version>1.14.4</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka_2.11</artifactId>
            <version>1.14.4</version>
        </dependency>
    </dependencies>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <java.version>1.8</java.version>
    </properties>

    <build>
        <plugins>
            <!--打包的插件-->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>2.4.3</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals><goal>shade</goal></goals>
                        <configuration>
                            <artifactSet>
                                <excludes>
                                    <exclude>org.slf4j:*</exclude>
                                    <exclude>log4j:*</exclude>
                                    <exclude>com.google.code.findbugs:jsr305</exclude>
                                </excludes>
                            </artifactSet>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>org.flink.demos.state.demo6_eos</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

## 错误总结

- 泛型丢失

lambda表达式简写时，导致collector泛型丢失，添加returns解决

```java
SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = streamSource.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
    String[] words = s.split("\\s+");
    for (String word : words) {
        if (word.length() > 0)
            collector.collect(Tuple2.of(word, 1));
    }
})
    //.returns(new TypeHint<Tuple2<String, Integer>>() {});
    //.returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
    //.returns(TypeInformation.of(Class.class));自定义类的class方法
    .returns(Types.TUPLE(Types.STRING,Types.INT));
```



The generic type parameters of 'Collector' are missing.

```
Exception in thread "main" org.apache.flink.api.common.functions.InvalidTypesException: The return type of function 'main(StreamBatch.java:26)' could not be determined automatically, due to type erasure. You can give type information hints by using the returns(...) method on the result of the transformation call, or by letting your function implement the 'ResultTypeQueryable' interface.

The generic type parameters of 'Collector' are missing. In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved. An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.FlatMapFunction' interface. Otherwise the type has to be specified explicitly using type information.
```



- java17开发JDK版本不兼容 问题

Run—>EditConfigurations…—>Modify options—>Add VM options—>JVM options
在JVM options 内添加下面指令：

```java
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

## 
