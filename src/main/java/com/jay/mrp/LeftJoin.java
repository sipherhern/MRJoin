package com.jay.mrp;/**
 * Created by jay on 2018/6/29.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * @program: InnerJoin
 * @description:
 * @author: Mr.Wang
 * @create: 2018-06-29 15:52
 **/

public class LeftJoin {
    public static final String DELIMITER_SPACE = " ";

    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: LeftJoin");
            System.exit(2);
        }

        Job job = new Job(conf,"LeftJoin");
        job.setJarByClass(InnerJoin.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        System.exit(job.waitForCompletion(true)? 0 : 1);
    }

    public static class MapperClass extends Mapper<LongWritable,Text,Text,Text>{
        public void map(LongWritable key, Text value, Context context) throws InterruptedException,IOException{
            String line = value.toString();
            String[] col = line.split(DELIMITER_SPACE);
            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().toString();

            if(line == null || line == ""){
                return;
            }
            if(fileName.contains("DEP.txt")){
                String ID = col[0];
                String city = col[1];
                context.write(new Text(ID),new Text("a#" + city));
            }else if(fileName.contains("EMP.txt")){
                String ID = col[0];
                String year1 = col[1];
                String year2 = col[2];
                context.write(new Text(ID), new Text("b#" + year1 + DELIMITER_SPACE + year2));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text>{
        LinkedList<String> alist = new LinkedList<String>();
        LinkedList<String> blist = new LinkedList<String>();
        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException,IOException{
            for(Text text : values) {
                String val = text.toString();
                if (val.startsWith("a#")) {
                    alist.add(val.substring(2));
                } else if (val.startsWith("b#")) {
                    blist.add(val.substring(2));
                }
            }

            if(blist.size() == 0){
                blist.add("null");
            }

            for(String astr : alist){
                for(String bstr : blist){
                    context.write(key,new Text(astr + DELIMITER_SPACE + bstr));
                }
            }

            alist.clear();
            blist.clear();
        }
    }
}
