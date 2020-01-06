package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WorldCountWithDataStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.readTextFile("data/input.txt");

        DataStream<Tuple2<String, Integer>> outCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String world : s.split(" ")) {
                            out.collect(new Tuple2<String, Integer>(world, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1);

        outCounts.print();

        env.execute();
    }
}
