//package mayankraj.hw2.AuthorshipReducer
package reducers
import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.{FloatWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

import scala.collection.JavaConverters._

class AuthorShipReducer extends Reducer[Text, FloatWritable, Text, FloatWritable] with LazyLogging {
  override def reduce(key: Text, values: lang.Iterable[FloatWritable], context: Reducer[Text, FloatWritable, Text, FloatWritable]#Context): Unit = {
    logger.trace(s"Reducer invoked: reduce(key: ${key.toString}, values: ${values.asScala.map(_.get())}")
    //val multipleOutputs = new MultipleOutputs(context)

    val sum = values.asScala.fold(new FloatWritable(0))((a, b) => new FloatWritable(a.get() + b.get()))
    logger.info(s"Reducer emit: <$key, ${sum.get()}>")
    context.write(key, sum)
    //multipleOutputs.write(key,sum,key.toString)
  }

}

