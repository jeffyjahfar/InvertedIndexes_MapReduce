import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class InvertedIndexes {

    public class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException,InterruptedException
        {
            /*Get the name of the file using context.getInputSplit()method*/
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line=value.toString();
            //Split the line in words
            String words[]=line.split(" ");
            for(String s:words){
                //for each word emit word as key and file name as value
                context.write(new Text(s), new Text(fileName));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            /*Declare the Hash Map to store File name as key to compute and store number of times the filename is occurred for as value*/
            HashMap m=new HashMap();
            int count=0;
            for(Text t:values){
                String str=t.toString();
                /*Check if file name is present in the HashMap ,if File name is not present then add the Filename to the HashMap and increment the counter by one , This condition will be satisfied on first occurrence of that word*/
                if(m!=null &&m.get(str)!=null){
                    count=(int)m.get(str);
                    m.put(str, ++count);
                }else{
                    /*Else part will execute if file name is already added then just increase the count for that file name which is stored as key in the hash map*/
                    m.put(str, 1);
                }
            }
            /* Emit word and [file1→count of the word1 in file1 , file2→count of the word1 in file2 ………] as output*/
            context.write(key, new Text(m.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(InvertedIndexes.class);
        conf.setJobName("InvertedIndexes");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        String keywords = "";
        for(int i=0; i<args.length-2; i++){
            keywords = keywords + " "+args[i+2];
        }
        conf.set("keywords",keywords);
        JobClient.runJob(conf);
    }
}
