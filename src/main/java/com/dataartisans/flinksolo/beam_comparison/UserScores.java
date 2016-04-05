package com.dataartisans.flinksolo.beam_comparison;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * A batch job which calculates per-user score totals over a bounded set of input data.
 * */
public class UserScores {

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


	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple4<String, Integer, Integer, Long>> userScores = env
				.readTextFile(args[0])
				.flatMap(new InputParser())
				.groupBy(0)
				.sum(2);

		userScores.print();
	}
}
