package mayankraj.hw2.MainDriver

import mayankraj.hw2.AuthorshipMapper
import mayankraj.hw2.AuthorshipReducer

import mayankraj.hw2.mapreduce
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import mayankraj.hw2.mymapper.MyMapper
import mayankraj.hw2.myreducer.MyReducer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{FloatWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}


object MainDriver extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.trace(s"main(args: ${args.mkString})")

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
    job2.setMapperClass(classOf[AuthorshipMapper.AuthorshipMapper])
    job2.setCombinerClass(classOf[AuthorshipReducer.AuthorShipReducer])
    job2.setReducerClass(classOf[AuthorshipReducer.AuthorShipReducer])
    job2.setInputFormatClass(classOf[mapreduce.XmlInputFormat])
    job2.setOutputFormatClass(classOf[TextOutputFormat[Text, FloatWritable]])

    FileInputFormat.setInputPaths(job2, new Path(args(0)))

    val outputPath2= new Path(args(1)+ "abc")
    FileOutputFormat.setOutputPath(job2, outputPath2)

    // Clean output directory if it exists
    logger.trace("Removing any existing output files")
    val outputDirectoryDeleted1 = outputPath2.getFileSystem(conf).delete(outputPath2, true)
    if (outputDirectoryDeleted1) {
      logger.debug(s"Deleted existing output files")
    }
    else {
      logger.debug("Output path already clean")
    }

    // Run the job and wait for its completion
    logger.info("Submitting job and waiting for completion...")
    job2.waitForCompletion(true)
    logger.info("Job2 completed.")





  }
}
