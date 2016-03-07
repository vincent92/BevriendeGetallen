package nl.hu.hadoop.wordcount;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class BevriendeGetallen {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(BevriendeGetallen.class);

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		// Generate random number text file
		try {
			new NumberGenerator(inputPath + "/input.txt", 100000, 10000).createRandomNumberFile();
		} catch (Exception e) {
			System.out.println("File creation exception occured, " + e.getMessage());
		}

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(NumberMapper.class);
		job.setReducerClass(NumberReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		// job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}

class NumberGenerator {
	
	private String fileName;
	private int size;
	private int numberSize;
	
	NumberGenerator(String fileName, int size, int numberSize) {
		this.fileName = fileName;
		this.size = size;
		this.numberSize = numberSize;
	}
	
	public void createRandomNumberFile() throws FileNotFoundException, UnsupportedEncodingException {

		PrintWriter writer = new PrintWriter(fileName, "UTF-8");		
		for (int i = 0; i < size; i++) {
			int random = (int )(Math.random() * numberSize + 1);
			writer.print(random + " ");
		}
		writer.close();
	}
}

class NumberMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	
	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {

		String[] numbers = value.toString().split("\\s");

		for (String num : numbers) {
			
			int number = Integer.parseInt(num);
			int divideTotal = divideSum(number);
			
			context.write(new IntWritable(number), new IntWritable(divideTotal));
		}
	}
	
	public int divideSum(int number) {
		int total = 0;
		int index = 1;
		do {
			if(number % index == 0) total += index;
			index++;
		} while (index < number);
		return total;
	}
}

class NumberReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private HashMap<Integer, Integer> numbers = new HashMap<>();

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		//220 284
		//284 220
		
		int value = values.iterator().next().get();
		
		if (numbers.containsKey(value) && key.get() == numbers.get(value)) {
			context.write(new IntWritable(value), key);
		} else {
			numbers.put(key.get(), value);
		}
	}
}