import java.io.*;
import java.util.*;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Ratings implements Writable
{
    public int movieid;
    public int rating;
    
    Ratings () {}

    Ratings(int m, int r){
        movieid = m;
        rating = r;
    }

    public void write (DataOutput out) throws IOException{
        out.writeInt(movieid);
        out.writeInt(rating);
    }

    public void readFields ( DataInput in ) throws IOException {
        movieid = in.readInt();
        rating = in.readInt();
    }

}

class Title implements Writable
{
    public int movieid;
    public String year;
    public String title;
    
    Title () {}

    Title(int m, String y ,String t){
        movieid = m;
        year = y;
        title = t;
    }

    public void write (DataOutput out) throws IOException{
        out.writeInt(movieid);
        out.writeUTF(year);
        out.writeUTF(title);
    }

    public void readFields ( DataInput in ) throws IOException {
        movieid = in.readInt();
        year = in.readUTF();
        title = in.readUTF();

    }

}

class Result implements Writable{
    public String year;
    public String title;
    public double avg_rating;

    Result(String y, String t, double ar){
        year = y;
        title = t;
        avg_rating = ar;
    }

    public void write (DataOutput out) throws IOException{
        out.writeUTF(year);
        out.writeUTF(title);
        out.writeDouble(avg_rating);
    }

    public void readFields ( DataInput in ) throws IOException {
        year = in.readUTF();
        title = in.readUTF();
        avg_rating = in.readDouble();
    }
    public String toString () { return year+": "+title +"  "+avg_rating;}

}



class RatingTitle implements Writable {  
    public short tag;/*this is like a flag ,values 0 or 1 */
    // public int rating;
    // public String title;
    public Ratings rating;
    public Title title;

    /* put your code here */
    RatingTitle () {}
    RatingTitle ( Ratings r ) { tag = 0; rating = r; }
    RatingTitle ( Title t ) { tag = 1; title = t; }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        if (tag==0)
            rating.write(out);
        else title.write(out);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        if (tag==0) {
            rating = new Ratings();
            rating.readFields(in);
        } else {
            title = new Title();
            title.readFields(in);
        }
    }
  
}

public class Netflix {

    /* put your code here */
    public static class RatingsMapper extends Mapper<Object,Text,IntWritable,RatingTitle > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            try{
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int movie_id = s.nextInt();
            s.nextInt();/*skip user id */
            int movie_rating = s.nextInt();
            Ratings r = new Ratings(movie_id,movie_rating);
            context.write(new IntWritable(r.movieid),new RatingTitle(r));
            s.close();
            }
            catch (NoSuchElementException | NumberFormatException e){
                // Handle parsing errors, e.g., log the error and skip the input line
            System.err.println("Error parsing line: " + value.toString());
            }
        }
    }


    public static class TitleMapper extends Mapper<Object,Text,IntWritable,RatingTitle > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            try{
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int movie_id = s.nextInt();
            String year = s.next();
            String title = s.next();
            

            Title t = new Title(movie_id,year,title);
            context.write(new IntWritable(t.movieid),new RatingTitle(t));
            s.close();
            }
            catch (NoSuchElementException | NumberFormatException e){
                // Handle parsing errors, e.g., log the error and skip the input line
            System.err.println("Error parsing line: " + value.toString());
            }
        }
    }

    public static class ResultReducer extends Reducer<IntWritable,RatingTitle,IntWritable,Result> {
        static Vector<Ratings> rate = new Vector<Ratings>();
        static Vector<Title> title = new Vector<Title>();
        @Override
        public void reduce ( IntWritable key, Iterable<RatingTitle> values, Context context )
                           throws IOException, InterruptedException {
            
            
            rate.clear();
            title.clear();
            for (RatingTitle v: values){
                if (v.tag == 0)
                    rate.add(v.rating);
                else title.add(v.title);
            }

            if (!rate.isEmpty()) {

            double averageRating = calculateAverageRating();
            

            for ( Title d: title ){
                context.write(null, new Result(d.year, d.title, averageRating));
            }
        }

     }
            private double calculateAverageRating() {
                
                double sum = 0.0;
                int count = 0;
                for (Ratings r : rate) {
                        sum += r.rating;
                        count ++;
                    }

                    double roundedResult = Math.round(sum / count * 1e7) / 1e7;
            
                    return roundedResult;
                }
        
    }

    public static void main ( String[] args ) throws Exception {
        /* put your code here */
        Job job = Job.getInstance();
        job.setJobName("JoinJob");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Result.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(RatingTitle.class);
        job.setReducerClass(ResultReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,RatingsMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,TitleMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
