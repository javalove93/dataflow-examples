package com.example.dataflow;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * 단어들의 출현 회수와 길이를 각각 PCollection으로 구해서
 * 두 데이터를 CoGroupByKey로 Merge 하는 예제
 * 미완성
 * https://beam.apache.org/documentation/pipelines/design-your-pipeline/#multiple-transforms-process-the-same-pcollection
 */

public class WordCount6 {

	private static final Logger LOG = LoggerFactory.getLogger(WordCount6.class);

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
//			.apply(GroupByKey.<String, Integer>create())
			.apply(Sum.integersPerKey())
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
