package lab01_q6.Lab01;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;

public class https_response_comp {

	public static void main(String[] args) throws Exception,IOException {
		
		Job job = Job.getInstance();
		job.setJarByClass(https_response_comp.class);
		job.setJobName("Monthly Summary");

		String input = "C:\\Users\\admin\\eclipse-workspace\\lab01\\access_log.txt";
		String output = "C:\\Users\\admin\\eclipse-workspace\\lab01\\output6";
		

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
	 
		job.setMapperClass(https_responsemapper.class);
		job.setReducerClass(https_responsereducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
										
		System.exit(job.waitForCompletion(true)?0:1);		
		
	}

}

