package com.example.dataflow;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount4 {

	private static final Logger LOG = LoggerFactory.getLogger(WordCount4.class);

	static class ExtractWordsFn extends DoFn<String, String> {
		private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
		private final Distribution lineLenDist = Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

		@ProcessElement
		public void processElement(@Element String element, OutputReceiver<String> receiver) {
			lineLenDist.update(element.length());
			if (element.trim().isEmpty()) {
				emptyLines.inc();
			}

			// Split the line into words.
			String[] words = element.split("\\W+");

			// Output each word encountered into the output PCollection.
			for (String word : words) {
				if (!word.isEmpty()) {
					receiver.output(word);
				}
			}
		}
	}

	public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
		@Override
		public String apply(KV<String, Long> input) {
			return input.getKey() + ": " + input.getValue();
		}
	}

	public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
		@Override
		public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

			// Convert lines of text into individual words.
			PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

			// Count the number of times each word occurs.
			PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

			return wordCounts;
		}
	}

	public static void runJob() throws IOException, IllegalArgumentException {
		// Create the Pipeline object with the options we defined above.
		Pipeline pipeline = Pipeline.create();

		pipeline.apply("Reading CNN News", TextIO.read().from("examples/cnn_news.txt"))
				.apply("Word Count", new CountWords())
				.apply(MapElements.via(new FormatAsTextFn()))
				.apply("Write Word Count", TextIO.write().to("output/count.txt"));

		// Run the pipeline.
		pipeline.run().waitUntilFinish();
	}

	// 실행
	// ./test WordCount1
	public static void main(String[] args) throws IOException, IllegalArgumentException {
		runJob();
	}
}
