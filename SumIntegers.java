import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    JobConf conf;
    //        private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void configure( JobConf job ) {
        this.conf = job;
    }

    public void map(LongWritable docId, Text value, OutputCollector<Text, IntWritable> output,
                    Reporter reporter) throws IOException {

        //Tokenize the document
        String line = value.toString();
        //if each line contains multiple integers
        StringTokenizer tokenizer = new StringTokenizer(line);

        Integer sum = 0;
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            sum+=Integer.parseInt(word.toString());
        }

        //send all entries with the same key so that sum from all files are reduced. If different
        //keys are used, only the sums with same key gets reduced.
        output.collect(new Text("sum"),sum);
    }
}

public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                       Reporter reporter) throws IOException {

        Integer global_sum = 0;
        while(values.hasNext()){
            global_sum += values.next().get();
        }
        output.collect(key, new Text(global_sum));
    }
}
}
