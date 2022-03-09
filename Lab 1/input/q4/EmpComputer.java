
package lab01_q4.Lab01;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmpComputer {

	public static void main(String[] args) throws Exception {
	 
		Job job = Job.getInstance();
		job.setJarByClass(EmpComputer.class);
		job.setJobName("Average Salary");
		
		String input = "E:\\Semester 06\\Nosql\\No sql lab\\Lab 1\\input\\employee.csv";
		String output = "E:\\Semester 06\\Nosql\\No sql lab\\Lab 1\\input\\output_q4";
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
	 
		job.setMapperClass(EmpMapper.class);

	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}