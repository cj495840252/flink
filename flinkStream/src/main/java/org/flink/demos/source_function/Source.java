package org.flink.demos.source_function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LongValueSequenceIterator;

import java.util.Arrays;
import java.util.List;


public class Source {
    public static void main(String[] args) throws Exception {

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


        // 读取socket
        env.socketTextStream("localhost",8080,"\\s+",2);

        // PROCESS_COMTINUOUSLY文件变化，重新读取整个文件重新算，interval读取时间间隔
        env.readFile(new TextInputFormat(null),"path",
                FileProcessingMode.PROCESS_CONTINUOUSLY,1);
        env.execute();
    }
}