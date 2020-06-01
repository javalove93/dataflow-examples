package com.example.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface SampleOptions extends DataflowPipelineOptions {

	@Description("inputPath for Google Cloud Storage")
	@Validation.Required
	String getInputPath();

	void setInputPath(String value);

	@Description("outputPath for Google Cloud Storage")
	@Validation.Required
	String getOutputPath();

	void setOutputPath(String value);
	
	@Description("regEx for finding non-alphabet characters")
	@Validation.Required
	String getRegEx();
	
	void setRegEx(String value);
}
