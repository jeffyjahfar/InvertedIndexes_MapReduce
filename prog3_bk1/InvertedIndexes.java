import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class InvertedIndexes {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        JobConf conf;
//        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void configure( JobConf job ) {
            this.conf = job;
        }

        public void map(LongWritable docId, Text value, OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException {
            // retrieve # keywords from JobConf
            int argc = Integer.parseInt( conf.get( "argc" ) );
            int[] keyword_count = new int[argc];
            for(int i=0;i<argc;i++)
                keyword_count[i] = 0;
//            String keywords = conf.get( "keywords" );
            // get the current file name
            FileSplit fileSplit = ( FileSplit )reporter.getInputSplit( );
            String filename = "" + fileSplit.getPath( ).getName( );
            String line = value.toString();
//            String words[]=line.split(" ");
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                for(int i=0;i<argc;i++){
                    String keyword_i = conf.get( "keyword" + i );
                    if(word.toString().equals(keyword_i)){
                        keyword_count[i]+=1;
                    }
                }
            }
            for(int i=0;i<argc;i++){
                String keyword_i = conf.get( "keyword" + i );
                output.collect(new Text(keyword_i),new Text(filename+"_"+keyword_count[i]));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

//        Text termPrev;

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {
//            if ( termPrev != null && !key.equals( termPrev ) ) {
                String sum = "";
                while (values.hasNext()) {
                    sum += values.next().toString()+" ";
                }
                output.collect(key, new Text(sum));
//            }
//            termPrev = key.clone( );
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        JobConf conf = new JobConf(InvertedIndexes.class); // AAAAA is this program’s file name
        conf.setJobName("InvertedIndexes"); // BBBBB is a job name, whatever you like
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0])); // input directory name
        FileOutputFormat.setOutputPath(conf, new Path(args[1])); // output directory name
        conf.set( "argc", String.valueOf( args.length - 2 ) ); // argc maintains #keywords
        for ( int i = 0; i < args.length - 2; i++ )
            conf.set( "keyword" + i, args[i + 2] ); // keyword1, keyword2, ...
//        String keywords = "";
//        for ( int i = 0; i < args.length - 2; i++ )
//            keywords = keywords + " "+args[i+2];
//        conf.set( "keywords", keywords );
        JobClient.runJob(conf);
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Total Execution Time: "+ totalTime);
    }
}
