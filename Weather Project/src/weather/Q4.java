package weather;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q4 {
	// Main Method for execution
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration cf = new Configuration();
		String[] files = new GenericOptionsParser(cf, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = new Job(cf, "q4");
		j.setJarByClass(Q4.class);
		j.setMapperClass(MapClass.class);
		j.setReducerClass(ReduceClass.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);

	}

	// Map Class
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {

			String row = value.toString();
			String date = row.substring(6, 14).replaceAll(" ", "");
			String tempHigh = row.substring(38, 45).replaceAll(" ", "");
			String tempLow = row.substring(46, 53).replaceAll(" ", "");

			double highest = Double.parseDouble(tempHigh);
			double lowest = Double.parseDouble(tempLow);

			if (highest != -9999.0 && lowest != -9999.0) {
				con.write(new Text(date), new Text(tempHigh + ":" + tempLow));
			}

		}
	}

	// Reducer Class
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {

		LinkedHashMap<Text, Text> map = new LinkedHashMap<Text, Text>();

		public void reduce(Text text, Iterable<Text> values, Context con)
				throws IOException, InterruptedException {

			for (Text value : values) {
				map.put(new Text(text), new Text(value));
			}
		}

		public void cleanup(Context con) throws IOException,
				InterruptedException {
			con.write(new Text("Date"), new Text(
					"Max Temperature	Min Temperature"));

			for (Map.Entry<Text, Text> entry : map.entrySet()) {
				String[] temp = entry.getValue().toString().split(":");

				con.write(new Text(entry.getKey()), new Text(temp[0] + "	"
						+ temp[1]));
			}
		}
	}
}
