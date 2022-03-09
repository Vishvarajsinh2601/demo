package lab01_q4.Lab01;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpMapper extends Mapper<LongWritable, Text, Text,Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
	   throws IOException, InterruptedException {
		//emp-no,name, dno, salary, state, gender
		String line = value.toString();
		String[] record = line.split(",");
		String dep = record[2];
		String name =record[1];
		String state= record[4];
		String gender= record[5];
		String sal= record[3];
		String empno=record[0];
		int salary = Integer.parseInt( record[3]);
		
		String s=name+" "+dep+" "+sal+" "+state+" "+gender;
		if ( salary > 100000 && dep.equals("5") )
			context.write(new Text(empno),new Text(s));
	}

}