package mayankraj


import mappers._
import reducers._


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{FloatWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import mayankraj.hw2.mapreduce
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging


/**
 * This is the maindriver program to run all the jobs in mapreduce .
 */
object MainDriver extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.trace(s"main(args: ${args.mkString})")
    logger.debug("Configuring MapReduce Job...")

    if (args.length <2){
      logger.error("Input and/or output paths not provided.")
      System.exit(-1)
    }
    //
    val conf = new Configuration()
    //
    val configuration = ConfigFactory.load()
    conf.set(mapreduce.XmlInputFormat.start_tag_key,configuration.getString("START_TAGS"))
    conf.set(mapreduce.XmlInputFormat.end_tag_key,configuration.getString("END_TAGS"))

    conf.set(
      "io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization")
    // configure the job


    val job = Job.getInstance(conf,"CoauthorsBins")
    job.setJarByClass(this.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setMapperClass(classOf[MyMapper])
    job.setCombinerClass(classOf[MyReducer])
    job.setReducerClass(classOf[MyReducer])
    job.setInputFormatClass(classOf[mapreduce.XmlInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(job, new Path(args(0)))

    val outputPath = new Path(args(1))
    FileOutputFormat.setOutputPath(job, outputPath)

    // Clean output directory if it exists
    logger.trace("Removing any existing output files")
    logger.trace("Removing any existing output files")
    val outputDirectoryDeleted = outputPath.getFileSystem(conf).delete(outputPath, true)
    if (outputDirectoryDeleted) {
      logger.debug(s"Deleted existing output files")
    }
    else {
      logger.debug("Output path already clean")
    }

    // Run the job and wait for its completion
    logger.info("Submitting job and waiting for completion...")
    job.waitForCompletion(true)
    logger.info("Job completed.")


    ///job2

    val conf1 = new Configuration()
    conf1.set(mapreduce.XmlInputFormat.start_tag_key,configuration.getString("START_TAGS"))
    conf1.set(mapreduce.XmlInputFormat.end_tag_key,configuration.getString("END_TAGS"))

    conf1.set(
      "io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization")
    // configure the job


    val job2 = Job.getInstance(conf,"CoauthorshipScores")
    job2.setJarByClass(this.getClass)
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[FloatWritable])
    job2.setMapperClass(classOf[AuthorshipMapper])
    job2.setCombinerClass(classOf[AuthorShipReducer])
    job2.setReducerClass(classOf[AuthorShipReducer])
    job2.setInputFormatClass(classOf[mapreduce.XmlInputFormat])
    job2.setOutputFormatClass(classOf[TextOutputFormat[Text, FloatWritable]])

    FileInputFormat.setInputPaths(job2, new Path(args(0)))

    val outputPath2= new Path(args(1)+ "2")
    FileOutputFormat.setOutputPath(job2, outputPath2)

    // Clean output directory if it exists
    logger.trace("Removing any existing output files")


    // Run the job and wait for its completion
    logger.info("Submitting job and waiting for completion...")
    job2.waitForCompletion(true)
    logger.info("Job2 completed.")



    val job3 = Job.getInstance(conf,"AuthorBins Stratified by year, journal")
    job3.setJarByClass(this.getClass)
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[IntWritable])
    job3.setMapperClass(classOf[AuthorsYearJournalFilterMapper])
    job3.setCombinerClass(classOf[AuthorsYearJournalFilterReducer])
    job3.setReducerClass(classOf[AuthorsYearJournalFilterReducer])
    job3.setInputFormatClass(classOf[mapreduce.XmlInputFormat])
    job3.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(job3, new Path(args(0)))

    val outputPath3 = new Path(args(1)+ "3")
    FileOutputFormat.setOutputPath(job3, outputPath3)

    // Clean output directory if it exists
    logger.trace("Removing any existing output files")

    // Run the job and wait for its completion
    logger.info("Submitting job and waiting for completion...")
    job3.waitForCompletion(true)
    logger.info("Job3 completed.")



    ////JOb4
    val job4 = Job.getInstance(conf,"MaxMeanMedian")
    job4.setJarByClass(this.getClass)
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputValueClass(classOf[IntWritable])
    //setting no of reducer to 1 to carry the aggregation functions
    job4.setNumReduceTasks(1)
    job4.setMapperClass(classOf[MaxMeanMapper])
    //job4.setCombinerClass(classOf[MaxMeanReducer])
    job4.setMapOutputKeyClass(classOf[Text])
    job4.setMapOutputValueClass(classOf[IntWritable])
    job4.setReducerClass(classOf[MaxMeanReducer])
    job4.setInputFormatClass(classOf[mapreduce.XmlInputFormat])
    job4.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(job4, new Path(args(0)))

    val outputPath4= new Path(args(1)+ "4")
    FileOutputFormat.setOutputPath(job4, outputPath4)

    // Clean output directory if it exists
    logger.trace("Removing any existing output files")


    // Run the job and wait for its completion
    logger.info("Submitting job and waiting for completion...")
    job4.waitForCompletion(true)
    logger.info("Job4 completed.")


    //job5
    val job5 = Job.getInstance(conf,"MaxMeanMedianStratified")
    job5.setJarByClass(this.getClass)
    job5.setOutputKeyClass(classOf[Text])
    job5.setOutputValueClass(classOf[IntWritable])
    //setting no of reducer to 1 to carry the aggregation functions
    job5.setNumReduceTasks(1)
    job5.setMapperClass(classOf[MaxMeanMapperStratified])
    //job4.setCombinerClass(classOf[MaxMeanReducer])
    job5.setMapOutputKeyClass(classOf[Text])
    job5.setMapOutputValueClass(classOf[IntWritable])
    job5.setReducerClass(classOf[MaxMeanReducer])
    job5.setInputFormatClass(classOf[mapreduce.XmlInputFormat])
    job5.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(job5, new Path(args(0)))

    val outputPath5= new Path(args(1)+ "5")
    FileOutputFormat.setOutputPath(job5, outputPath5)

    // Clean output directory if it exists
    logger.trace("Removing any existing output files")


    // Run the job and wait for its completion
    logger.info("Submitting job and waiting for completion...")
    job5.waitForCompletion(true)
    logger.info("Job5 completed.")

  }
}
