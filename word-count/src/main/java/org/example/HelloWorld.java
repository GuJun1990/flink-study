package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HelloWorld {

    public static void main(String[] args) throws Exception {
        // 1. StreamExecutionEnvironment是所有Flink程序的基础：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 一般来说， 只需要调用getExecutionEnvironment()方法，因为Flink会根据不同的上下文来做对应的事情，
        // 若是在IDE中，那么它会创建一个本地环境，即在本地机器上执行你的程序。如果编译了一个JAR文件出来，并通过命令行提交，
        // 那么Flink集群manager会执行你的main方法，而getExecutionEnvironment()会返回一个集群环境，即在集群上执行程序

        // 2. Source
        // 执行环境（execution environment）有多种读取文件的方法用来指定数据源：可以一行一行来读取，比如csv文件，
        // 或使用自定义的输入格式。 可以通过下列方式，来按行来读取一个文本文件：
//        DataStream<String> text = env.readTextFile("data/input.txt");
        DataStream<Integer> ints = env.fromElements(1, 2, 3, 4, 5); // 可以从Kafka，ActiveMQ，文件，socket等数据源获取数据

        // 3. 数据转换
        // 调用 DataStream 上的转换函数来对数据进行处理：
        DataStream<Integer> myInts = ints.map(new MapFunction<Integer, Integer>() {
            public Integer map(Integer integer) throws Exception {
                return 2 * integer;
            }
        });

        // 4. Sink
        // 指定计算结果的存储地方
        // 一旦得到了最终结果的DataStream，便可以通过创建Sink将它写入到外部系统中。
        myInts.print();

        // 5. 启动程序执行
        // 一旦完成程序，用户需要启动程序执行，可以直接调用execute()， 根据不同的ExecutionEnvironment类型，
        // 将会决定本地模式执行或是提交到集群上执行。 execute()函数会返回JobExecutionResult，
        // 包含执行的次数和累加器（accumulator）结果。 所有的Flink程序都是延迟执行的：当执行程序的main函数时，
        // 数据加载了但转换并没有马上执行。 相反，每个操作都被创建并加入到了程序执行计划中。
        // 当调用ExecutionEnvironment的execute()时， 这些操作才被真正执行。
        env.execute();
    }

}
