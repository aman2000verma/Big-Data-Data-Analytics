package weather;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

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
		j.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);

	}

	// Map Class
	public static class MapClass extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {

			String row = value.toString();
			String date = row.substring(6, 10).replaceAll(" ", "");
			String tempStr = row.substring(38, 45).replaceAll(" ", "");
			double temp = Double.parseDouble(tempStr);

			if (temp != -9999.0)
				con.write(new Text(date), new DoubleWritable(temp));

		}
	}

	// Reducer Class
	public static class ReduceClass extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text text, Iterable<DoubleWritable> values,
				Context con) throws IOException, InterruptedException {

			double highest = Double.NEGATIVE_INFINITY;
			for (DoubleWritable value : values) {
				if (value.get() > highest)
					highest = value.get();
			}

			con.write(new Text("Highest temperature for year " + text + " = "),
					new DoubleWritable(highest));

		}
	}
}
