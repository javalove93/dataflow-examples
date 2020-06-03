package com.example.dataflow;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikipediaProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(WikipediaProcessor.class);

	public static class CleansingProcessor extends DoFn<String, String> {

		private String name;
		private Pattern pattern;

		public CleansingProcessor(String name, String regex) {
			this.name = name;
			pattern = Pattern.compile(regex);
		}

		@ProcessElement
		public void processElement(ProcessContext ctx) throws IllegalArgumentException {
			String aRecord = ctx.element();
			Matcher matcher = pattern.matcher(aRecord);
			String result = matcher.replaceAll(" ");
			
			ctx.output(result);
		}
	}
	
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

	public static void runCleansingJob(SampleOptions options) throws IOException, IllegalArgumentException {
		FileSystems.setDefaultPipelineOptions(options);

		// Create the Pipeline object with the options we defined above.
		Pipeline pipeline = Pipeline.create(options);
		
		// Read Text files
		pipeline.apply("Read Text files", TextIO.read().from(options.getInputPath()))
				.apply("Cleansing",
						ParDo.of(new CleansingProcessor("my processor", options.getRegEx())))
				.apply("Extract Words",
						ParDo.of(new ExtractWordsFn()))
				.apply("Count", Count.perElement())
				// PTransformation은 Anonymous 클래스로 작성할 수도 있음 
				.apply("Format", ParDo.of(new DoFn<KV<String, Long>, String>() {
					@ProcessElement
					public void processElement(ProcessContext ctx) throws IllegalArgumentException {
						KV<String, Long> aRecord = ctx.element();
						ctx.output(aRecord.getKey() + ": " + aRecord.getValue());
					}
				}))
				.apply("Write Text data", TextIO.write().to(options.getOutputPath()));

		// 동일한 파이프라인을 아래와 같이 풀어서 작성 가능
		// 파이프라인 중간에 결과로 나오는 PCollection에 대한 이해 쉬움
		// p2는 생성 후 실행하진 않음
		Pipeline p2 = Pipeline.create();
		PCollection<String> pcol = p2.apply("Read Text files", TextIO.read().from(options.getInputPath()))
				.apply("Cleansing",
						ParDo.of(new CleansingProcessor("my processor", options.getRegEx())))
				.apply("Extract Words",
						ParDo.of(new ExtractWordsFn()));
		PCollection<KV<String, Long>> pcol2 = pcol.apply("Count", Count.perElement());
		pcol2.apply("Format", ParDo.of(new DoFn<KV<String, Long>, String>() {
				@ProcessElement
				public void processElement(ProcessContext ctx) throws IllegalArgumentException {
					KV<String, Long> aRecord = ctx.element();
					ctx.output(aRecord.getKey() + ": " + aRecord.getValue());
				}
		}))
		.apply("Write Text data", TextIO.write().to(options.getOutputPath()));

		
		// Run the pipeline.
		pipeline.run().waitUntilFinish();
	}

	public static void main(String[] args) throws IOException, IllegalArgumentException {
		// Create and set your PipelineOptions.
		PipelineOptionsFactory.register(SampleOptions.class);
		SampleOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SampleOptions.class);

		System.out.println(options);
		runCleansingJob(options);
	}
}
