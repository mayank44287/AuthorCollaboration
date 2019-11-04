//import java.io.DataInput
//import java.io.DataOutput
//import java.io.IOException
//import org.apache.hadoop.io.Writable
//
//
//class MinMaxMean extends Writable{
//  private var median = new Float()
//  private var max = new Float()
//  private var average = new Float()
//  private var count = new Long()
//
//  def getMax(): Float ={
//    return this.max
//  }
//
//  def setMax(m: Float): Unit= {
//    this.max = m
//  }
//
//  def getMedian() :Float= {
//    return this.median
//  }
//
//  def setMedian(med: Float): Unit={
//    this.median = med
//  }
//
//  def getAverage :Unit = {
//    return this.average
//  }
//
//  def setAverage(av: Float): Unit = {
//    this.average = av
//  }
//
//  def getCount(): Unit = {
//    return this.count
//  }
//  def setCount(cnt: Long):Unit={
//    this.count = cnt
//  }
//
//  import java.io.DataInput
//  import java.io.DataOutput
//  import java.io.IOException
//
//  @throws[IOException]
//  def readFields(in: DataInput): Unit = {
//    median = in.readFloat()
//    max = in.readFloat()
//    count = in.readLong()
//  }
//
//  @throws[IOException]
//  def write(out: DataOutput): Unit = {
//    out.writeDouble(median)
//    out.writeDouble(max)
//    out.writeLong(count)
//  }
//
//  override def toString: String = median + "\t" + max + "\t" + count
//
//
//}
