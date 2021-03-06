package com.example.dataflow;

import java.io.IOException;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Regex;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount1 {

	private static final Logger LOG = LoggerFactory.getLogger(WordCount1.class);

	public static void runJob() throws IOException, IllegalArgumentException {
		// Create the Pipeline object with the options we defined above.
		Pipeline pipeline = Pipeline.create();

		PCollection<String> cnn_news = pipeline.apply("Reading CNN News", TextIO.read().from("examples/cnn_news.txt"));
		PCollection<String> words = cnn_news.apply(Regex.split("\\W+"));
//		PCollection<String> words = cnn_news.apply("Word Count", ParDo.of(new WordCountFn()));
		words.apply("Write Words", TextIO.write().to("output/words.txt"));
		
		// Run the pipeline.
		pipeline.run().waitUntilFinish();
	}

	public static void main(String[] args) throws IOException, IllegalArgumentException {
		runJob();
	}
}
