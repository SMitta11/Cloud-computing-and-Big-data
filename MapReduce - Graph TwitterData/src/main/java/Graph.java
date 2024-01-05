import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;




class Tagged implements Writable {
    public boolean tag;                // true for a graph vertex, false for distance
    public int distance;               // the distance from the starting vertex
    public Vector<Integer> following;  // the vertex neighbors

    Tagged () { tag = false; }
    Tagged ( int d ) { tag = false; distance = d; }
    Tagged ( int d, Vector<Integer> f ) { tag = true; distance = d; following = f; }

    public void write ( DataOutput out ) throws IOException {
        out.writeBoolean(tag);
        out.writeInt(distance);
        if (tag) {
            out.writeInt(following.size());
            for ( int i = 0; i < following.size(); i++ )
                out.writeInt(following.get(i));
        }
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readBoolean();
        distance = in.readInt();
        if (tag) {
            int n = in.readInt();
            following = new Vector<Integer>(n);
            for ( int i = 0; i < n; i++ )
                following.add(in.readInt());
        }
    }
}

public class Graph {
    static int start_id = 14701391;
    static int max_int = Integer.MAX_VALUE;

    /* ... */



    public static class MyMapper1 extends Mapper<Object,Text,IntWritable,IntWritable>
    {
        @Override
        public void map (Object key, Text value, Context context) throws  IOException,InterruptedException
        {
     
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int user_id = s.nextInt();
            int follower_id = s.nextInt();
            context.write(new IntWritable(follower_id), new IntWritable(user_id));
         
            s.close();
    }
    
}

    public static class MyReducer1 extends Reducer<IntWritable, IntWritable, IntWritable, Tagged>
    {
        @Override
        public void reduce (IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            // Vector  following = new Vector();
            Vector following = new Vector();

            for (IntWritable id : values)
            {
                following.add(id.get());
            };
            if (key.get() == start_id || key.get() == 1 )

                context.write(key,new Tagged(0,following));
            else
                context.write(key, new Tagged(max_int,following));
          
        
            
        }
    } 



        public static class MyMapper2 extends Mapper<IntWritable,Tagged,IntWritable,Tagged>
        {
            @Override
            public void map(IntWritable key, Tagged value, Context context)throws IOException, InterruptedException {
                context.write(key,new Tagged(value.distance, value.following));
                if (value.distance < max_int)
                {
                    for (int id : value.following)
                    {
                        context.write(new IntWritable(id) , new Tagged(value.distance + 1));
                        // context.write(new IntWritable(key.get()) , new Tagged(value.distance + 1));
                    }
                }
            
        }
    }

        public static class MyReducer2 extends Reducer<IntWritable, Tagged, IntWritable, Tagged>
        {
            @Override
            public void reduce (IntWritable key, Iterable<Tagged> values, Context context) throws IOException, InterruptedException
            {
                int m = max_int;
                // Vector <Integer> following = null;
                Vector <Integer> following = new Vector<Integer>();
                for (Tagged value : values)
                {
                    if(value.distance < m)
                    
                        m = value.distance;
                    
                    if(value.tag)
                    
                        following = value.following;
                    
                }
            
                context.write(key,new Tagged(m, following));

            }
        }



    public static class MyMapper3 extends Mapper<IntWritable,Tagged,IntWritable,IntWritable>
    {
        @Override
        public void map(IntWritable key, Tagged value, Context context)throws IOException, InterruptedException {

            if (value.distance < max_int)
            {
                context.write(key,new IntWritable(value.distance));
            }
    }
}


    public static void main ( String[] args ) throws Exception {
        int iterations = 5;
        Job job = Job.getInstance();
        /* ... First Map-Reduce job to read the graph */
        /* ... */
        job.setJobName("MyJob1");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Tagged.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReducer1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < iterations; i++ ) {
            job = Job.getInstance();
            /* ... Second Map-Reduce job to calculate shortest distance */
            /* ... */
            job.setJobName("MyJob2");
            job.setJarByClass(Graph.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Tagged.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Tagged.class);
            job.setMapperClass(MyMapper2.class);
            job.setReducerClass(MyReducer2.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job,new Path(args[1]+i));
            FileOutputFormat.setOutputPath(job,new Path(args[1]+(i+1)));
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        /* ... Last Map-Reduce job to output the distances */
        /* ... */
        job.setJobName("MyJob3");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper3.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[1]+"5"));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
