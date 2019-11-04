//package mayankraj.hw2.AuthorshipMapper


package mappers

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io.{FloatWritable, IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.xml.{Elem, XML}
import org.w3c.dom.NodeList

import scala.collection.mutable.ArrayBuffer

/**
 * this mapper emits the authorship score for authors in the publication based on the following formula
 * To compute the authorship score you will use the following formula. The total score for a paper is one. Hence,
 * if an author published 15 papers as the single author without any co-authors, then her score will be 15.
 * For a paper with multiple co-authors the score will be computed using a split weight as the following. First,
 * ach co-author receives 1/N score where N is the number of co-authors. Then, the score of the last co-author is
 * credited 1/(4N) leaving it 3N/4 of the original score. The next co-author to the left is debited 1/(4N) and the
 * process repeats until the first author is reached. For example, for a single author the score is one. For two authors,
 * the original score is 0.5. Then, for the last author the score is updated 0.53/4 = 0.5-0.125 = 0.375 and the first author's
 * score is 0.5+0.125 = 0.625. The process repeats the same way for N co-authors.
 */

class AuthorshipMapper extends Mapper[LongWritable,Text,Text,FloatWritable] with LazyLogging {
  val configSettings =  ConfigFactory.load()
  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  val authorshipScore = new FloatWritable(0)

  val authorkey = new Text()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, FloatWritable]#Context): Unit = {
    //
    logger.trace(s"Mapper invoked: map(key: $key, value: ${value.toString}")

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
      if (n == 1){
        authorkey.set(new Text((list1(0))))
        authorshipScore.set(score)
        context.write(authorkey,authorshipScore)
      }
      else {
        for (i <- list1.indices) {

          if (i == 0) {

            authorkey.set(new Text(list1(i)))
            authorshipScore.set(scoreMain)
            context.write(authorkey, authorshipScore)
          }
          else if (i == (list1.length - 1)) {

            authorkey.set(new Text(list1(i)))
            authorshipScore.set(scoreLast)
            context.write(authorkey, authorshipScore)
          }
          else {
            authorkey.set(new Text(list1(i)))
            authorshipScore.set(score)
            context.write(authorkey, authorshipScore)
          }
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
