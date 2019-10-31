package mayankraj.hw2.myreducer

import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.{FloatWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs


import scala.collection.JavaConverters._

class MyReducer extends Reducer[Text, IntWritable, Text, IntWritable] with LazyLogging {


  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    logger.trace(s"Reducer invoked: reduce(key: ${key.toString}, values: ${values.asScala.map(_.get())}")

    val multipleOutputs = new MultipleOutputs(context)

    // Sum up all the values for the same faculty pair
    val sum = values.asScala.fold(new IntWritable(0))((a, b) => new IntWritable(a.get() + b.get()))
    logger.info(s"Reducer emit: <$key, ${sum.get()}>")
    context.write(key, sum)
    //multipleOutputs.write(key,sum,key.toString)
  }

  override def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

  }
}
