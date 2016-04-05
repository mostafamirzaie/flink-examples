package com.dataartisans.flinksolo.simple;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamingWordCount {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> dataStream = env
				.socketTextStream("localhost", 9999)
				.flatMap(new Splitter())
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				.sum(1);

		//dataStream.print();

		env.execute("Socket Stream WordCount");
	}

	public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (String word: sentence.split(" ")) {
				System.out.println(word);
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}

}
