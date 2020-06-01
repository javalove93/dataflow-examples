package com.example.dataflow;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
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

	public static void runCleansingJob(SampleOptions options) throws IOException, IllegalArgumentException {
		FileSystems.setDefaultPipelineOptions(options);

		// Create the Pipeline object with the options we defined above.
		Pipeline pipeline = Pipeline.create(options);
		
		// Read Text files
		pipeline.apply("Read Text files", TextIO.read().from(options.getInputPath()))
				.apply("Cleansing",
						ParDo.of(new CleansingProcessor("my processor", options.getRegEx())))
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
