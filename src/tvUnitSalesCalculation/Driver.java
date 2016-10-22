package tvUnitSalesCalculation;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;




public class Driver {
	
	public static void main(String [] args) throws Exception{
		
		if(args.length != 3)
			throw new Exception("Invalid arguments. usage <InputFile>"
					+ "<OutputPat> <TaskType: 2/3>");
		if(!args[2].equals("2") && !args[2].equals("3"))
			throw new Exception("Task type should be either 2 or 3");
		//create the configuration object
		Configuration conf = new Configuration();
		//set the task type
		conf.set("TaskType",args[2]);
		
		Job job = Job.getInstance(conf);
		
		//set the proper task name
		if(args[2].equals("2"))
			job.setJobName("Total Unit Sold per Company");
		else
			job.setJobName("Total Unit Sold per State for Onida");
		
		job.setJarByClass(Driver.class);
		
		//set class for map task
		job.setMapperClass(MapTask.class);
		
		//set class for reduce task
		job.setReducerClass(ReduceTask.class);
		
		//set output key and value for map task
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//set output key and value for reducer Task
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//set input and output formatter class
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//set reducer task count
		job.setNumReduceTasks(1);
		
		//set input and out files
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//wait until it completes all the task
		job.waitForCompletion(true);
	}

}
