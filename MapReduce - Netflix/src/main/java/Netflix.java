import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Netflix 
{

    /* put your Map-Reduce methods here */

    public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable>
    {
        @Override
        public void map (Object key, Text value, Context context) throws  IOException,InterruptedException
        {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int movieid = s.nextInt();
            //s.nextInt(); /*Skip next column of userid */
            int users_rated_movie = s.nextInt();
            context.write(new IntWritable(movieid), new IntWritable(users_rated_movie));
            s.close();

        }
    }

    public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
        @Override
        public void reduce (IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int count = 0;
            for (IntWritable v : values)
            {
                count ++;

            };
            context.write(key, new IntWritable (count));

        }
    } 
    
    public static class MyMapper2 extends Mapper<Object,Text,Text,DoubleWritable>
    {
        @Override
        public void map (Object key, Text value, Context context) throws  IOException,InterruptedException
        {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int movieid = s.nextInt();
            s.nextInt();/*skip user id */
            double rating = s.nextDouble();
            String date = s.next();
            String[] extract_year_from_date = date.split("-");

            String year = extract_year_from_date[0]; // Extract the first part as the year


            String string_movieid = Integer.toString(movieid);

            String new_key_movieid_year = string_movieid + "-" + year;
         
            
            context.write(new Text(new_key_movieid_year), new DoubleWritable(rating));
            s.close();
           
        }
    }


    public static class MyReducer2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        @Override
        public void reduce (Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
            double sum = 0.0;
            int count = 0;
            for (DoubleWritable v : values)
            {
                sum += v.get();
                count ++;

            };
            // Calculate the rounded result to 7 decimal places
            double roundedResult = Math.round(sum / count * 1e7) / 1e7;

            context.write(key, new DoubleWritable(roundedResult));

        }
    } 



    public static void main ( String[] args ) throws Exception {
        
        
        /* put your main program here */
        Job job1 = Job.getInstance();
	    job1.setJobName("job1");
	    job1.setJarByClass(Netflix.class);
	    job1.setOutputKeyClass(IntWritable.class);
	    job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
	    job1.setMapOutputValueClass(IntWritable.class);
	    job1.setMapperClass(MyMapper.class);
	    job1.setReducerClass(MyReducer.class);
	    job1.setInputFormatClass(TextInputFormat.class);
	    job1.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.setInputPaths(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	    job1.waitForCompletion(true);


        Job job2 = Job.getInstance();
	    job2.setJobName("job2");
	    job2.setJarByClass(Netflix.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(DoubleWritable.class);
	    job2.setMapperClass(MyMapper2.class);
	    job2.setReducerClass(MyReducer2.class);
	    job2.setInputFormatClass(TextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.setInputPaths(job2, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	    //job2.waitForCompletion(true);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        

        }
    

   
}

