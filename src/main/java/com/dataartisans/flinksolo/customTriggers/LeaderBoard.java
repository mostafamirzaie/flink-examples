package com.dataartisans.flinksolo.customTriggers;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LeaderBoard {

	private static final EventTimeTriggerWithEarlyAndLateFiring trigger =
			EventTimeTriggerWithEarlyAndLateFiring.create()
					.withEarlyFiringEvery(Time.minutes(10))
					.withLateFiringEvery(Time.minutes(5))
					.withAllowedLateness(Time.minutes(20))
					.accumulating();

	private static class TopK implements WindowFunction<Iterable<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>, String, TimeWindow> {

		private final int K;

		public TopK(int K) {
			Preconditions.checkArgument(K >= 1);
			this.K = K;
		}

		@Override
		public void apply(String key, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> tuple2s, Collector<List<Tuple2<String, Integer>>> collector) throws Exception {
			List<Tuple2<String, Integer>> topK = new ArrayList<Tuple2<String, Integer>>();
			for (Tuple2<String, Integer> tuple2 : tuple2s) {
				topK.add(tuple2);
			}

			Collections.sort(topK, Collections.<Tuple2<String,Integer>>reverseOrder());
			List<Tuple2<String, Integer>> top10 = new ArrayList<Tuple2<String, Integer>>();
			for (int i = 0; i < K; i++) {
				top10.add(topK.get(i));
			}
			collector.collect(top10);
		}
	}

	private static DataStream<List<Tuple2<String, Integer>>> getLeaderBoard(DataStream<Tuple2<String, Integer>> input) {
		return input
				.keyBy(0)
				.window(TumblingTimeWindows.of(Time.hours(1)))
				.trigger(trigger)
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
						return new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1);
					}})
				.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
					@Override
					public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
						return "Void";
					}})
				.window(TumblingTimeWindows.of(Time.hours(1)))
				.trigger(trigger)
				.apply(new TopK(10)).setParallelism(1);
	}

	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<String> input =  env.socketTextStream("localhost", 9999);
		DataStream<Tuple2<String, Integer>> usersAndScores =  HourlyTeamScores.extractUserScores(input);

		DataStream<List<Tuple2<String, Integer>>> topKUsers = LeaderBoard.getLeaderBoard(usersAndScores);
		topKUsers.writeAsText("/Users/kkloudas/Desktop/leaderBoard.txt");
	}
}
