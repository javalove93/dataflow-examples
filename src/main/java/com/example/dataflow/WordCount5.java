package com.example.dataflow;

import java.io.IOException;
import java.util.Iterator;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount5 {

	private static final Logger LOG = LoggerFactory.getLogger(WordCount5.class);

	public static class WordCountFn extends DoFn<String, KV<String, Integer>> {

		public WordCountFn() {
			//
		}

		@ProcessElement
		public void processElement(ProcessContext ctx) throws IllegalArgumentException {
			String aRecord = ctx.element();
			String[] result = aRecord.split("\\W+");
			
			for(String s : result) {
				ctx.output(KV.of(s, 1));
			}
		}
	}

	public static void runJob() throws IOException, IllegalArgumentException {
		// Create the Pipeline object with the options we defined above.
		Pipeline pipeline = Pipeline.create();

		pipeline.apply("Reading CNN News", TextIO.read().from("examples/cnn_news.txt"))
			.apply("Word Count", ParDo.of(new WordCountFn()))
			.apply(GroupByKey.<String, Integer>create())
			.apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>>() {
				@ProcessElement
				public void processElement(ProcessContext ctx) throws IllegalArgumentException {
					KV<String, Iterable<Integer>> aRecord = ctx.element();
					Iterator<Integer> it = aRecord.getValue().iterator();
					int count = 0;
					while(it.hasNext()) {
						count = count + it.next();
					}
					
					ctx.output(KV.of(aRecord.getKey(), count));
				}
			}))
			.apply(ParDo.of(new DoFn<KV<String, Integer>, String>() {
				@ProcessElement
				public void processElement(ProcessContext ctx) throws IllegalArgumentException {
					KV<String, Integer> aRecord = ctx.element();
					ctx.output(aRecord.getKey() + ": " + aRecord.getValue());
				}
			}))
			.apply("Write Word Count", TextIO.write().to("output/count.txt"));
		
		// Run the pipeline.
		pipeline.run().waitUntilFinish();
	}

	//	실행
	//	./test WordCount1
	public static void main(String[] args) throws IOException, IllegalArgumentException {
		runJob();
	}
}
