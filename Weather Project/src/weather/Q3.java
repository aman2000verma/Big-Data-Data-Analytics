package weather;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

public class Q3 {

	// Main Method for execution
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration cf = new Configuration();
		String[] files = new GenericOptionsParser(cf, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = new Job(cf, "q3");
		j.setJarByClass(Q3.class);
		j.setMapperClass(MapClass.class);
		j.setReducerClass(ReduceClass.class);
		j.setOutputKeyClass(DoubleWritable.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);

	}

	// Map Class
	public static class MapClass extends
			Mapper<LongWritable, Text, DoubleWritable, Text> {

		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {

			String row = value.toString();
			String date = row.substring(6, 14).replaceAll(" ", "");
			String tempHigh = row.substring(38, 45).replaceAll(" ", "");
			String tempLow = row.substring(46, 53).replaceAll(" ", "");

			double high = Double.parseDouble(tempHigh);
			double low = Double.parseDouble(tempLow);

			if (low != -9999.0) {
				con.write(new DoubleWritable(low), new Text(date));
			}

			if (high != -9999.0) {
				con.write(new DoubleWritable(high), new Text(date));
			}

		}
	}

	// Reducer Class
	public static class ReduceClass extends
			Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		LinkedHashMap<DoubleWritable, Text> lowMap = new LinkedHashMap<DoubleWritable, Text>();
		LinkedHashMap<DoubleWritable, Text> highMap = new LinkedHashMap<DoubleWritable, Text>();
		int i = 0;

		public void reduce(DoubleWritable key, Iterable<Text> values,
				Context con) throws IOException, InterruptedException {

			double temp = key.get();
			String date = values.iterator().next().toString();

			// Input for Reduce comes sorted by default, so temperatures are
			// going to be in ascending order

			// For Top 10 Coldest Days, we need the first 10 key/value pairs
			// from the reduce input
			if (i < 10) {
				lowMap.put(new DoubleWritable(temp), new Text(date));
				++i;
			}

			// For Top 10 Hottest Days, we need the last 10 key/value pairs from
			// the reduce input
			highMap.put(new DoubleWritable(temp), new Text(date));
			if (highMap.size() > 10) {
				// Delete the first K/V
				highMap.remove(highMap.keySet().iterator().next());
			}

		}

		public void cleanup(Context con) throws IOException,
				InterruptedException {

			con.write(null, new Text("Top 10 Coldest Days:"));
			for (Map.Entry<DoubleWritable, Text> m : lowMap.entrySet()) {
				con.write(m.getKey(), m.getValue());
			}

			con.write(null, new Text("Top 10 Hottest Days:"));
			List<DoubleWritable> highKeys = new ArrayList<DoubleWritable>(
					highMap.keySet());
			Collections.reverse(highKeys);

			for (int i = 0; i < highKeys.size(); i++) {
				con.write(highKeys.get(i), highMap.get(highKeys.get(i)));
			}
		}

	}

}
