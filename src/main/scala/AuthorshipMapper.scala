package mayankraj.hw2.AuthorshipMapper


import mayankraj.hw2.mapreduce
import javax.xml.parsers.DocumentBuilderFactory
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io.{FloatWritable, IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.xml.{Elem, XML}
import org.w3c.dom.NodeList

import scala.collection.mutable.ArrayBuffer



class AuthorshipMapper extends Mapper[LongWritable,Text,Text,FloatWritable] with LazyLogging {
  val configSettings =  ConfigFactory.load()
  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  val authorshipScore = new FloatWritable(0)

  val authorkey = new Text()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, FloatWritable]#Context): Unit = {
    //
    logger.trace(s"mapper invoked")
    val xmlString = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + value.toString + "</dblp>"

    val publElement = XML.withSAXParser(xmlParser).loadString(xmlString)

    val authorLookupTag = publElement.child.head.label match {
      case "book" | "proceedings" => "editor"
      case _ => "author"
    }

    val authors = (publElement \\ authorLookupTag).map(author=>author.text).toList

    if (authors.nonEmpty){
      var list1 = new ArrayBuffer[String]
      generateAuthorBins(authors).foreach{author =>
        list1 += author


      }

      val n = list1.length.toFloat
      val score  = (1/n)
      val credit = 0.25/n
      val scoreLast = (score - credit).toFloat
      val scoreMain = (score + credit).toFloat
      for (i <-list1.indices){

        if (i==0){

          authorkey.set(new Text(list1(i)))
          authorshipScore.set(scoreMain)
          context.write(authorkey, authorshipScore)
        }
        else if(i == (list1.length -1)){

          authorkey.set(new Text(list1(i)))
          authorshipScore.set(scoreLast)
          context.write(authorkey, authorshipScore)
        }
        else{
          authorkey.set(new Text(list1(i)))
          authorshipScore.set(score)
          context.write(authorkey, authorshipScore)
        }
      }

      list1.clear() //clear the list to load new record
    }


  }

  def generateAuthorBins(author: Seq[String]): Iterator[String]  = {
    author.size match {
      // If the list of faculty is empty, return an empty list
      case 0 => Iterator()
      // If there is only one faculty, return only the faculty
      //case 1 => author.iterator
      // If there are more than one faculty, return all the individual faculty names, along with all possible
      // combinations of the faculty, with each pair joined by the `edgeIndicator`
      case _ => author.iterator
    }
  }

}
