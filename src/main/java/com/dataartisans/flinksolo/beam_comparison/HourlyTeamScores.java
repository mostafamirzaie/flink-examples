package com.dataartisans.flinksolo.beam_comparison;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.util.Collector;

/**
 * A job which calculate per-team scores, bucketed into one-hour fixed windows, on a bounded dataset.
 * */
public class HourlyTeamScores {

	private static class InputParser implements FlatMapFunction<String, Tuple4<String, Integer, Integer, Long>> {

		@Override
		public void flatMap(String s, Collector<Tuple4<String, Integer, Integer, Long>> collector) throws Exception {
			// we assume that the input is userId, teamId, score, timestamp
			String[] tokens = s.split("\\s");
			if(tokens.length != 4) {
				throw new RuntimeException("Unknown input format.");
			}
			collector.collect(new Tuple4<>(tokens[0], Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]), Long.parseLong(tokens[3])));
		}
	}

	private static class TimestampAssigner extends AscendingTimestampExtractor<Tuple4<String, Integer, Integer, Long>> {

		@Override
		public long extractAscendingTimestamp(Tuple4<String, Integer, Integer, Long> input) {
			return input.f3;
		}
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple4<String, Integer, Integer, Long>> teamHourlyScores = env
				.readTextFile(args[0])
				.flatMap(new InputParser())
				.assignTimestampsAndWatermarks(new TimestampAssigner())
				.keyBy(1)
				.window(TumblingEventTimeWindows.of(Time.seconds(20)))
				.trigger(EventTimeTrigger.create())
				.sum(2);

		teamHourlyScores.print();
		env.execute("Hourly Team Scores.");
	}
}
