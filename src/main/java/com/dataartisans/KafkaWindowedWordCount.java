package com.dataartisans;

import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.io.UnboundedFlinkSource;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.io.UnboundedSocketSource;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KafkaWindowedWordCount {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaWindowedWordCount.class);
	static final int WINDOW_SIZE = 1;  // Default window duration in minutes

	static class AddTimestampFn extends DoFn<KV<String, Long>, KV<String, Long>> {
		private static final long RAND_RANGE = 7200000; // 2 hours in ms

		@Override
		public void processElement(ProcessContext c) {
			long randomTimestamp = System.currentTimeMillis()
					- (int) (Math.random() * RAND_RANGE);
			c.outputWithTimestamp(c.element(), new Instant(randomTimestamp));
		}
	}

	static class FormatAsStringFn extends DoFn<KV<String, Long>, String> {
		@Override
		public void processElement(ProcessContext c) {
			String row = c.element().getKey() + " - " + c.element().getValue() + " @ " + c.timestamp().toString();
			System.out.println(row);
			c.output(row);
		}
	}

	static class ExtractWordsFn extends DoFn<String, String> {
		private final Aggregator<Long, Long> emptyLines =
				createAggregator("emptyLines", new Sum.SumLongFn());

		@Override
		public void processElement(ProcessContext c) {
			if (c.element().trim().isEmpty()) {
				emptyLines.addValue(1L);
			}

			// Split the line into words.
			String[] words = c.element().split("[^a-zA-Z']+");

			// Output each word encountered into the output PCollection.
			for (String word : words) {
				if (!word.isEmpty()) {
					c.output(word);
				}
			}
		}
	}

	public static interface StreamingWordCountOptions extends com.dataartisans.flink.dataflow.examples.WordCount.Options {
		@Description("Fixed window duration, in minutes")
		@Default.Integer(WINDOW_SIZE)
		Integer getWindowSize();

		void setWindowSize(Integer value);

		@Description("Whether to run the pipeline with unbounded input")
		boolean isUnbounded();

		void setUnbounded(boolean value);
	}

	public static void main(String[] args) throws IOException {
		StreamingWordCountOptions options = PipelineOptionsFactory.create().as(StreamingWordCountOptions.class);
		options.setJobName("Smaller");
		options.setUnbounded(true);
		options.setStreaming(true);
		options.setRunner(FlinkPipelineRunner.class);

		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		Pipeline pipeline = Pipeline.create(options);

		// this is the flink consumer
		FlinkKafkaConsumer082 kafkaConsumer = new FlinkKafkaConsumer082<>(
				parameterTool.getRequired("topic"),
				new SimpleStringSchema(),
				parameterTool.getProperties());

		// Convert lines of text into individual words.
		PCollection<String> words = pipeline
				.apply(Read.from(new UnboundedFlinkSource<String, UnboundedSource.CheckpointMark>(options, kafkaConsumer)).named("StreamingWordCount"))
				.apply(ParDo.of(new ExtractWordsFn()))
				.apply(Window.<String>into(SlidingWindows.of(Duration.standardSeconds(60)).every(Duration.standardSeconds(10)))
						.triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO)
						.discardingFiredPanes());
////				.apply(ParDo.of(new AddTimestampFn()));

		PCollection<KV<String, Long>> wordCounts =
				words.apply(Count.<String>perElement());

		wordCounts.apply(ParDo.of(new FormatAsStringFn()))
				.apply(TextIO.Write.to("./out.txt"));

		pipeline.run();
	}
}
