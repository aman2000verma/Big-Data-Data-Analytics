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

public class Q2 {

	// Main Method for execution
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration cf = new Configuration();
		String[] files = new GenericOptionsParser(cf, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = new Job(cf, "q2");
		j.setJarByClass(Q2.class);
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
			String date = row.substring(6, 14).replaceAll(" ", "");
			String tempHigh = row.substring(38, 45).replaceAll(" ", "");
			String tempLow = row.substring(46, 53).replaceAll(" ", "");

			double highest = Double.parseDouble(tempHigh);
			double lowest = Double.parseDouble(tempLow);

			if (lowest < 10.0 && highest < 35.0)
				con.write(new Text(date + " Cold Day "), new DoubleWritable(
						lowest));

			else if (highest > 35.0)
				con.write(new Text(date + " Hot Day "), new DoubleWritable(
						highest));

		}
	}

	// Reducer Class
	public static class ReduceClass extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void setup(Context con) throws IOException, InterruptedException {
			con.write(new Text("Date	Hot/Cold	Max Temp"), null);
		}

		public void reduce(Text text, Iterable<DoubleWritable> values,
				Context con) throws IOException, InterruptedException {

			for (DoubleWritable value : values) {
				double temp = value.get();
				if (temp != -9999.0)
					con.write(text, new DoubleWritable(temp));
			}

		}
	}

}
