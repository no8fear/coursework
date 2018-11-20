import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

// The main class is defined, which is later used as the JAR class

public class AnagramRetriever {


	// The mapper portion of the program is defined
	
	public static class MapperAnagram extends Mapper<Object, Text, Text, Text>{
		private Text sortedKey = new Text();
		private Text mapperOutput = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			StringTokenizer tokenizer = new StringTokenizer(value.toString(),
			" \t\n\r\f,.:()!?", false);
			
			while(tokenizer.hasMoreTokens()){

				// Remove all non letter characters, and sort the word array

				String word = tokenizer.nextToken();
				word = word.replaceAll("[^A-Za-z]","");
				char[] charArray = word.toCharArray();
				Arrays.sort(charArray);

				// Set both the sorted key, and the mapped output

				sortedKey.set(new String(charArray));
				mapperOutput.set(word);

				context.write(sortedKey, mapperOutput);
			}
		}	
	}

	// The reducer portion of the program is defined
	
	public static class ReducerAnagram extends Reducer<Text, Text, Text, Text>{
		private Text reducerOutput = new Text();
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
			StringBuffer anagramsList = new StringBuffer();
			
			// Anagrams are added concatenated
			for(Text anagramText: value){
				String tempVal = anagramText.toString();

				// Check to see that the word is unique, as there were multiple occurences of the same word.

				if (!anagramsList.toString().contains(tempVal)) {
				anagramsList.append(anagramText+", ");

				}
			}
			reducerOutput.set(new String(anagramsList));

			String[] parts = reducerOutput.toString().split(" ");

			if (parts.length >= 2){
				context.write(key, reducerOutput);
			}
		}
	}
	

	public static void main(String[] args) throws IllegalArgumentException, IOException, InterruptedException, ClassNotFoundException {


		Job job = Job.getInstance();
		
		job.setJarByClass(AnagramRetriever.class);

		// The mapper and reducer classes are defined

		job.setMapperClass(MapperAnagram.class);
		job.setReducerClass(ReducerAnagram.class);

		// The below functions are necessary if the reducer outputs do no match the mapper outputs
		
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Object.class);
		job.setOutputValueClass(Text.class);
		
		//input path as first argument
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//output path as second argument
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);	
	}

}