package com.dataartisans.flinksolo.customTriggers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.util.Collector;

public class HourlyTeamScores {

	public static DataStream<Tuple2<String, Integer>> extractUserScores(DataStream<String> input) {
		return input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
				if(!s.isEmpty()) {
					String[] tokens = s.split("\\s");
					collector.collect(new Tuple2<String, Integer>(tokens[0],
							tokens.length == 1 ? 0 : Integer.parseInt(tokens[1])));
				}
			}
		});
	}

	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<String> input =  env.socketTextStream("localhost", 9999);
		DataStream<Tuple2<String, Integer>> usersAndScores =  HourlyTeamScores.extractUserScores(input);
		DataStream<Tuple2<String, Integer>> scoresPerUser = usersAndScores
				.keyBy(0)
				.window(TumblingTimeWindows.of(Time.hours(1)))
				.trigger(EventTimeTrigger.create())
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
						return new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1);
					}
				});
		scoresPerUser.writeAsText("/Users/kkloudas/Desktop/hourlyScoresPerUser.txt");
	}
}
