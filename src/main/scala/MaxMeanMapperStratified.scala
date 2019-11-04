package mappers

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io.{FloatWritable, IntWritable, LongWritable, Text}


import scala.xml.XML
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper


class MaxMeanMapperStratified extends Mapper[LongWritable,Text,Text,IntWritable] with LazyLogging{
  val one  = new IntWritable(1)

  val authorkey = new Text()

  val configSettings =  ConfigFactory.load()
  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  val settings = ConfigFactory.load()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.trace(s"Mapper invoked: map(key: $key, value: ${value.toString}")
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + value.toString + "</dblp>"

    val xmlElement = XML.withSAXParser(xmlParser).loadString(xmlString)


    val lookupTag = xmlElement.child.head.label match {
      case "book" | "proceedings" => "editor"
      case _ => "author"
    }

    val year = (xmlElement \\ "year").map(year => year.text).toList

    val p = year filter settings.getStringList("YEAR").contains
    if (p.nonEmpty) {
      val authors = (xmlElement \\ lookupTag).map(author => author.text.toLowerCase.trim).toList
      var binSize = 0
      if (authors.nonEmpty) {
        authors.foreach { i =>
          context.write(new Text(i), new IntWritable(authors.size))
        }
      }
  }
  }


}
