package lab01_q1.Lab01;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmpSumComputer {

	public static void main(String[] args) throws Exception {
	 
		Job job = Job.getInstance();
		job.setJarByClass(EmpSumComputer.class);
		job.setJobName("Sum of Salary");
		
		String input = "E:\Semester 06\Nosql\No sql lab\Lab 1\input\employee.csv";
		String output = "E:\Semester 06\Nosql\No sql lab\Lab 1\input\output";
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
	 
		job.setMapperClass(EmpSumMapper.class);
		job.setReducerClass(EmpSumReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}