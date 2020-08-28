package sales;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//Q.1 Find out number of products sold in each country.    
public class Q1 {

	// Main Method for execution
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration cf = new Configuration();
		String[] files = new GenericOptionsParser(cf, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = new Job(cf, "q1");
		j.setJarByClass(Q1.class);
		j.setMapperClass(MapClass.class);
		j.setReducerClass(ReduceClass.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);

	}

	// Map Class
	public static class MapClass extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {

			String row = value.toString().replaceAll(" ", "").toUpperCase();
			String[] cells = row.split(",");
			String text = cells[7].toString().trim();

			con.write(new Text(text), new IntWritable(1));
		}
	}

	// Reducer Class
	public static class ReduceClass extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text text, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			con.write(text, new IntWritable(sum));
		}
	}

}
