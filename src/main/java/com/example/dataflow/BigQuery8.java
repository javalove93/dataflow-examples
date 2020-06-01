package com.example.dataflow;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.dataflow.WordCount6.WordCountFn;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

/*
 * BigQuery 조회하여 언어별 회수 Count
 * https://beam.apache.org/documentation/io/built-in/google-bigquery/
 */

public class BigQuery8 {

	private static final Logger LOG = LoggerFactory.getLogger(BigQuery8.class);

	public static class BigQueryFn extends DoFn<TableRow, KV<String, Integer>> {

		public BigQueryFn() {
			//
		}

		@ProcessElement
		public void processElement(ProcessContext ctx) throws IllegalArgumentException {
			TableRow row = ctx.element();
			String language = (String)row.get("language");
			ctx.output(KV.of(language, 1));
		}
	}

	public static void runJob() throws IOException, IllegalArgumentException {
		PipelineOptions options = PipelineOptionsFactory.create();
		options.setTempLocation("gs://javalove93-samples-us-central1");
		
		// Create the Pipeline object with the options we defined above.
		Pipeline pipeline = Pipeline.create(options);

		TableReference tableSpec = new TableReference()
				.setProjectId("api-project-249965614499")
				.setDatasetId("workshop")
				.setTableId("word_count");
		
		TableSchema tableSchema =
			    new TableSchema()
			        .setFields(
			            ImmutableList.of(
			                new TableFieldSchema()
			                    .setName("word")
			                    .setType("STRING")
			                    .setMode("REQUIRED"),
			                new TableFieldSchema()
			                    .setName("count")
			                    .setType("INTEGER")
			                    .setMode("REQUIRED")));

		
		pipeline.apply("Reading CNN News", TextIO.read().from("examples/cnn_news.txt"))
				.apply("Word Count", ParDo.of(new WordCountFn()))
				.apply(Sum.integersPerKey())
				.apply(ParDo.of(new DoFn<KV<String, Integer>, TableRow>() {
					@ProcessElement
					public void processElement(ProcessContext ctx) throws IllegalArgumentException {
						KV<String, Integer> aRecord = ctx.element();
						TableRow row = new TableRow()
								.set("word", aRecord.getKey())
								.set("count", aRecord.getValue());
						
						ctx.output(row);
					}
				}))
				.apply("Write to BigQuery", BigQueryIO.writeTableRows()
			            .to(tableSpec)
			            .withSchema(tableSchema)
			            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
			            .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));
		
		// Run the pipeline.
		pipeline.run().waitUntilFinish();
	}

	//	실행
	//	./test WordCount1
	public static void main(String[] args) throws IOException, IllegalArgumentException {
		runJob();
	}
}
