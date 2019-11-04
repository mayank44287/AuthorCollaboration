package reducers

import java.lang
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.{FloatWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._


class MaxMeanReducer extends Reducer[Text, IntWritable, Text, Text] with LazyLogging{
  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit ={
    val valueList = new ListBuffer[Integer]

    //iterate over values
    var sum = 0
    values.forEach{ value =>
      valueList += value.get()
      sum += value.get()
    }


    val average = sum.toFloat/valueList.size

    logger.info("This is for author" + key.toString)
    val sortedList = valueList.sorted
    sortedList.foreach { i =>
      logger.info(i.toString)
    }
    val mid = sortedList.size / 2

    val median = if (sortedList.size % 2== 1) sortedList(mid) else ((sortedList(mid) + sortedList(mid - 1) ).toFloat/2)

    val maximum = sortedList(sortedList.size - 1)
    val minimum = sortedList(0)
    logger.info("The min value is" + minimum.toString)

    //val avg = sum / values.asScala.size.toFloat
    logger.info("Reducer output")
    context.write(key, new Text(minimum.toString + "," + maximum.toString + "," + average.toString + "," + median.toString))
  }

}
