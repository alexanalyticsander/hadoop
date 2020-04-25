package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4a {

    final String gtid = "asong49";

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    Text node = new Text();
    IntWritable plus = new IntWritable(1);
    IntWritable minus = new IntWritable(-1);
	
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

String line = value.toString();
String[] fields = line.split("\t");

String node1 = String.valueOf(fields[0]);
node.set(node1);
context.write(node, plus);
String node2 = String.valueOf(fields[1]);
node.set(node2);
context.write(node, minus);

}
}

public static class DegreeMapper extends Mapper<Object, Text, Text, IntWritable>{
private IntWritable degree = new IntWritable(1);
private Text freq = new Text();

public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
String [] lines = value.toString().split("\t");
freq.set(lines[1]);
context.write(freq, degree);

}
}

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	IntWritable result = new IntWritable();    

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
	int sum = 0;
      for (IntWritable value : values) {
      sum = sum + value.get();
    }
      result.set(sum);
      context.write(key, result);
}
}

  public static void main(String[] args) throws Exception {

    /* TODO: Update variable below with your gtid */

    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "Q4a1");

    /* TODO: Needs to be implemented */
    job1.setJarByClass(Q4a.class);
    job1.setMapperClass(TokenizerMapper.class);
    job1.setReducerClass(IntSumReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path("temp"));

    job1.waitForCompletion(true);
    Job job2 = Job.getInstance(conf, "Q4a2");

    job2.setJarByClass(Q4a.class);
    job2.setMapperClass(DegreeMapper.class);
    job2.setReducerClass(IntSumReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path("temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);

  }
}
