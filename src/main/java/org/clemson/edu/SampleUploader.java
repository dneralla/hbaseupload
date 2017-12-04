package org.clemson.edu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class SampleUploader extends Configured implements Tool {

    private static final String NAME = "SampleUploader";

    /**
     * Job configuration.
     */
    public static Job configureJob(Configuration conf, String[] args)
            throws IOException {
        Path inputPath = new Path(args[0]);
        String tableName = args[1];
        Job job = new Job(conf, NAME + "_" + tableName);
        job.setJarByClass(Uploader.class);
        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, inputPath);
        job.setMapperClass(Uploader.class);
        // No reducers.  Just write straight to table.  Call initTableReducerJob
        // because it sets up the TableOutputFormat.
        TableMapReduceUtil.initTableReducerJob(tableName, null, job);
        job.setNumReduceTasks(0);
        return job;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(HBaseConfiguration.create(), new SampleUploader(), args);
        System.exit(status);
    }

    /**
     * Main entry point.
     *
     * @param otherArgs The command line parameters after ToolRunner handles standard.
     * @throws Exception When running the job fails.
     */
    public int run(String[] otherArgs) throws Exception {
        if (otherArgs.length != 2) {
            System.err.println("Wrong number of arguments: " + otherArgs.length);
            System.err.println("Usage: " + NAME + " <input> <tablename>");
            return -1;
        }
        Job job = configureJob(getConf(), otherArgs);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    static class Uploader
            extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        private long checkpoint = 100;
        private long count = 0;

        @Override
        public void map(LongWritable key, Text line, Context context)
                throws IOException {

            // Input is a CSV file
            // Each map() is a single line, where the key is the line number
            // Each line is comma-delimited; row,family,qualifier,value

            // Split CSV line
            String[] values = line.toString().split(",");
            if (values.length != 4) {
                return;
            }

            // Extract each value
            byte[] row = Bytes.toBytes(values[0]);
            byte[] family = Bytes.toBytes(values[1]);
            byte[] qualifier = Bytes.toBytes(values[2]);
            byte[] value = Bytes.toBytes(values[3]);

            // Create Put
            Put put = new Put(row);
            put.addColumn(family, qualifier, value);

            // Uncomment below to disable WAL. This will improve performance but means
            // you will experience data loss in the case of a RegionServer crash.
            // put.setWriteToWAL(false);

            try {
                context.write(new ImmutableBytesWritable(row), put);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Set status every checkpoint lines
            if (++count % checkpoint == 0) {
                context.setStatus("Emitting Put " + count);
            }
        }
    }
}