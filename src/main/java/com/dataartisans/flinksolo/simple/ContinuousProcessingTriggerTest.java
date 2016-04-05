package com.dataartisans.flinksolo.simple;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class ContinuousProcessingTriggerTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(2);

		env.readFileStream(args[0], 100, FileMonitoringFunction.WatchType.PROCESS_ONLY_APPENDED)
				.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

					@Override
					public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
						System.out.println("Task " + getRuntimeContext().getIndexOfThisSubtask() +
								" of "+ getRuntimeContext().getNumberOfParallelSubtasks());

						for(String str: s.toLowerCase().split("\\s")) {
							collector.collect(new Tuple2<String, Integer>(str, 1));
						}
					}
				}).keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.of(10, TimeUnit.SECONDS)))
				//.trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
				.sum(1).print();
		env.execute();
	}
}
