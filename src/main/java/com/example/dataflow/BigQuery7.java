package com.example.dataflow;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;

/*
 * BigQuery 조회하여 언어별 회수 Count
 * https://beam.apache.org/documentation/io/built-in/google-bigquery/
 */

public class BigQuery7 {

	private static final Logger LOG = LoggerFactory.getLogger(BigQuery7.class);

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

		/*
		 * year	INTEGER	NULLABLE	
		 * month	INTEGER	NULLABLE	
		 * day	INTEGER	NULLABLE	
		 * wikimedia_project	STRING	NULLABLE	
		 * language	STRING	NULLABLE	
		 * title	STRING	NULLABLE	
		 * views	INTEGER	NULLABLE	
		 */
		TableReference tableSpec = new TableReference()
				.setProjectId("bigquery-samples")
				.setDatasetId("wikipedia_benchmark")
				.setTableId("Wiki100k");
		
//		pipeline.apply("Reading from BigQuery", BigQueryIO.read(
//				(SchemaAndRecord elem) -> (Double) elem.getRecord().get("max_temperature"))
//				.fromQuery(
//		                "SELECT max_temperature FROM [clouddataflow-readonly:samples.weather_stations]")
//				.usingStandardSql()    
//				.withCoder(DoubleCoder.of()))

		pipeline.apply("Reading from BigQuery", BigQueryIO.readTableRows().from(tableSpec))
				.apply("Language Count", ParDo.of(new BigQueryFn()))
				.apply(Sum.integersPerKey())
				.apply(ParDo.of(new DoFn<KV<String, Integer>, String>() {
					@ProcessElement
					public void processElement(ProcessContext ctx) throws IllegalArgumentException {
						KV<String, Integer> aRecord = ctx.element();
						ctx.output(aRecord.getKey() + ": " + aRecord.getValue());
					}
				}))
				.apply("Write Language Count", TextIO.write().to("output/bigquery.txt"));
		
		// Run the pipeline.
		pipeline.run().waitUntilFinish();
	}

	//	실행
	//	./test WordCount1
	public static void main(String[] args) throws IOException, IllegalArgumentException {
		runJob();
	}
}
