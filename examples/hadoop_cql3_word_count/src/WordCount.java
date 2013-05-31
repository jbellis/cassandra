/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.hadoop.cql3.ColumnFamilyOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.cql3.CQLConfigHelper;
import org.apache.cassandra.hadoop.cql3.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.nio.charset.CharacterCodingException;

/**
 * This counts the occurrences of words in ColumnFamily Standard1, that has a single column (that we care about)
 * "text" containing a sequence of words.
 *
 * For each word, we output the total number of occurrences across all texts.
 *
 * When outputting to Cassandra, we write the word counts as a {word, count} column/value pair,
 * with a row key equal to the name of the source column we read the words from.
 */
public class WordCount extends Configured implements Tool
{
    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

    static final String KEYSPACE = "cql3_worldcount";
    static final String COLUMN_FAMILY = "inputs";

    static final String OUTPUT_REDUCER_VAR = "output_reducer";
    static final String OUTPUT_COLUMN_FAMILY = "output_words";

    private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count";
    
    private static final String CONF_COLUMN_NAME = "columnname";

    public static void main(String[] args) throws Exception
    {
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(0);
    }

    public static class TokenizerMapper extends Mapper<List<IColumn>, Map<ByteBuffer, IColumn>, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ByteBuffer sourceColumn;

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
        {
        }

        public void map(List<IColumn> keys, Map<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        {
            for (IColumn column : columns.values())
            {
                String name  = ByteBufferUtil.string(column.name());
                String value = ByteBufferUtil.string(column.value());
                               
                logger.debug("read {}:{}={} from {}",
                             new Object[] {toString(keys), name, value, context.getInputSplit()});

                StringTokenizer itr = new StringTokenizer(value);
                while (itr.hasMoreTokens())
                {
                    word.set(itr.nextToken());
                    context.write(word, one);
                }
            }
        }
        
        private String toString(List<IColumn> keys)
        {
            String result = "";
            try
            {
                for (IColumn column : keys)
                    result = result + ByteBufferUtil.string(column.value()) + ":";
            
            }
            catch (CharacterCodingException e)
            {
                logger.error("Failed to print keys", e);
            }
            return result;
        }
    }

    public static class ReducerToFilesystem extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }
    
    public static class ReducerToCassandra extends Reducer<Text, IntWritable, ByteBuffer, List<List<ByteBuffer>>>
    {
        private ByteBuffer outputKey;

        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
        throws IOException, InterruptedException
        {
            outputKey = ByteBufferUtil.bytes(context.getConfiguration().get(CONF_COLUMN_NAME));
        }

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(outputKey, Collections.singletonList(getBindVariables(word, sum)));
        }

        private List<ByteBuffer> getBindVariables(Text word, int sum)
        {
            List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
            variables.add(outputKey);
            variables.add(ByteBufferUtil.bytes(word.toString()));
            variables.add(ByteBufferUtil.bytes(String.valueOf(sum)));         
            return variables;
        }
    }

    public int run(String[] args) throws Exception
    {
        String outputReducerType = "filesystem";
        if (args != null && args[0].startsWith(OUTPUT_REDUCER_VAR))
        {
            String[] s = args[0].split("=");
            if (s != null && s.length == 2)
                outputReducerType = s[1];
        }
        logger.info("output reducer type: " + outputReducerType);

        Job job = new Job(getConf(), "wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);

        if (outputReducerType.equalsIgnoreCase("filesystem"))
        {
            job.setCombinerClass(ReducerToFilesystem.class);
            job.setReducerClass(ReducerToFilesystem.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));
        }
        else
        {
            job.setReducerClass(ReducerToCassandra.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(ByteBuffer.class);
            job.setOutputValueClass(List.class);

            job.setOutputFormatClass(ColumnFamilyOutputFormat.class);

            ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);
            job.getConfiguration().set(CONF_COLUMN_NAME, "sum");
            String query = "INSERT INTO " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY +
                           " (row_id, word, count_num) " +
                           " values (?, ?, ?)";
            CQLConfigHelper.setOutputPreparedStatement(job.getConfiguration(), query);
            ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
            ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        }

        job.setInputFormatClass(ColumnFamilyInputFormat.class);

        ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");

        CQLConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");
        
        //this is the user defined filter clauses, you can comment it out if you want count all titles
        CQLConfigHelper.setInputWhereClauses(job.getConfiguration(), "title='A'");
        
        job.waitForCompletion(true);
        return 0;
    }
}
