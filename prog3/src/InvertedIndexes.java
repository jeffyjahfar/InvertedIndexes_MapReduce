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

            // retrieve number of keywords from Context
            int argc = Integer.parseInt( conf.get( "argc" ) );
            int[] keyword_count = new int[argc];

            //set initial count of all keywords within the document as 0
            for(int i=0;i<argc;i++)
                keyword_count[i] = 0;

            // get the current file name
            FileSplit fileSplit = ( FileSplit )reporter.getInputSplit( );
            String filename = "" + fileSplit.getPath( ).getName( );

            //Tokenize the document
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            //For each token, increment the count of keyword if the token is same as the keyword
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                for(int i=0;i<argc;i++){
                    String keyword_i = conf.get( "keyword" + i );
                    if(word.toString().equals(keyword_i)){
                        keyword_count[i]+=1;
                    }
                }
            }
            /*
            Once count of all keywords in the document is computed, collect as key value pair
            of (keyword, filename_count) provided the count of the keyword in the document is non-zero
            */
            for(int i=0;i<argc;i++){
                String keyword_i = conf.get( "keyword" + i );
                if(keyword_count[i] > 0)
                    output.collect(new Text(keyword_i),new Text(filename+"_"+keyword_count[i]));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {

                /*Declare the Hash Map to store File name as key to compute and store number of times the filename is occurred for as value*/
                HashMap<String,Integer> map=new HashMap<String,Integer>();
                int count=0;
                while(values.hasNext()){
                    String[] str=values.next().toString().split("_");
                    if(map.containsKey(str[0])){
                        Integer curr_count = map.get(str[0]);
                        str[1] = str[1].replaceAll("\\s","");
                        curr_count += Integer.parseInt(str[1]);
                        map.put(str[0],curr_count);
                    }else{
                        str[1] = str[1].replaceAll("\\s","");
                        map.put(str[0],Integer.parseInt(str[1]));
                    }
                }

                //Sorting the hashmap containing filenames and counts based on counts
                List<java.util.Map.Entry<String,Integer>> list = new ArrayList<>(map.entrySet());
                list.sort(java.util.Map.Entry.comparingByValue());

                HashMap<String,Integer> result=new HashMap<String,Integer>();
                for (java.util.Map.Entry<String,Integer> entry : list) {
                    result.put(entry.getKey(), entry.getValue());
                }

                //append the filename_count Texts from all files for each keyword
                String sum = "";

                for(String filename : result.keySet()) {
                        sum += filename+"_"+map.get(filename)+" ";
                }
                output.collect(key, new Text(sum));
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
        JobClient.runJob(conf);

        //Calculate total execution time
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Total Execution Time: "+ totalTime);
    }
}
