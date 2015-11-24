val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.Vectors
import breeze.stats.DescriptiveStats._
import breeze.linalg._
import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

def isNumeric(input: String): Boolean = {
    if (input == "") {false}
    else {input.forall(x => (x.isDigit || x == '.' || x =='E' || x == 'e')) && input.count(_ == '.')<2 && (input.count(_ == 'E') + input.count(_ == 'e')) <2}
}
def toDouble(s: String, default:Double=Double.NaN) = {if (isNumeric(s)) s.toDouble else default}
//def isHeader(line: Array[String], header: Array[String]) = line.deep == header.deep

def removeHeader(input: org.apache.spark.rdd.RDD[String]) = {
    val header = input.first
    val output = input.filter(line => !(line == header))
    output
}

def matrixToRdd(m: org.apache.spark.mllib.linalg.Matrix) : org.apache.spark.rdd.RDD[(Long, org.apache.spark.mllib.linalg.DenseVector)] = {
    val cols = m.toArray.grouped(m.numRows)
    val rows = cols.toSeq.transpose
    val vectors = rows.zipWithIndex.map{case (row, index) => (index.toLong, new org.apache.spark.mllib.linalg.DenseVector(row.toArray))}
    sc.parallelize(vectors)
}


def print(input: org.apache.spark.rdd.RDD[String]) = input.take(10).foreach(println)


// Load and parse the data

class SimpleCSVHeader(header:Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def apply(array:Array[String], key:String):String = array(index(key))
}


//0case class schema(time: String, 
//1                  year: Double,
//2                  month: Double,
//3                  day: Double,
//4                  hour: Double,
//5                  minute: Double,
//6                  second: Double,
//7                  tdur: Double, 
//8                  sip: String, 
//9                  dip: String,
//10                  sport: Double,
//11                  dport: Double, 
//12                  proto: String, 
//13                  flag: String, 
//14                  fwd: Double, 
//15                  stos: Double,
//16                  ipkt: Double, 
//17                  ibyt: Double, 
//18                  opkt: Double, 
//19                  obyt: Double, 
//20                  input: Double, 
//21                  output: Double,
//22                  sas: String, 
//23                  das: Sring, 
//24                  dtos: String, 
//25                  dir: String,
//26                  rip: String)

//----------Inputs-------------
//val file = "/user/history/hiveflow/netflow/year=2015/month=6/day=18/hour=0/*"
val file = System.getenv("DPATH")
//val output_file = "/user/history/hiveflow/netflow/word_counts_for_20150618"
val output_file = System.getenv("HPATH") + "/word_counts"
//val output_file_for_lda = "/user/history/hiveflow/netflow/lda_word_counts_for_20150618"
val output_file_for_lda = System.getenv("HPATH") + "/lda_word_counts"
val compute_quantiles : Boolean = false
val quant = Array(0.1,0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
val cuts_input = system.getenv("CUT")
var ibyt_cuts : Array[Double] = cuts_input.split(",")(0).split(" ").map(_.toDouble)
var ipkt_cuts : Array[Double] = cuts_input.split(",")(1).split(" ").map(_.toDouble)
var time_cuts : Array[Double] = cuts_input.split(",")(2).split(" ").map(_.toDouble)

//-----------------------------

def compute_ecdf(x : org.apache.spark.rdd.RDD[Double]) : org.apache.spark.rdd.RDD[(Double, Double)] ={
    val counts = x.map( v => (v,1)).reduceByKey(_+_).sortByKey().cache()
    val partSums: Array[Double] = 0.0 +: counts.mapPartitionsWithIndex {
      case (index, partition) => Iterator(partition.map { case (sample, count) => count }.sum.toDouble)
    }.collect()    
    val numValues = partSums.sum    
    val sumsRdd = counts.mapPartitionsWithIndex {
      case (index, partition) => {
        var startValue = 0.0
        for (i <- 0 to index) {
          startValue += partSums(i)
        }
        partition.scanLeft((0.0, startValue))((prev, curr) => (curr._1, prev._2 + curr._2)).drop(1)
      }
    }
    sumsRdd.map( elem => (elem._1, elem._2 / numValues))
}

def quantiles(quantiles: Array[Double], ecdf: org.apache.spark.rdd.RDD[(Double, Double)]): Array[Double] ={
    var result = Array.fill[Double](quantiles.length)(0.0)
    val ps : Array[(Double, Double)]= ecdf.collect()
    scala.util.Sorting.quickSort(ps)
    var pos : Int = 0
    for ((v,p) <- ps){
        if (pos<result.length){
            if (p<quantiles(pos)){result(pos) = v
            }else pos = pos + 1 }
        }
    result
}


val rawdata = sc.textFile(file)

val datanoheader = removeHeader(rawdata)
val sample = datanoheader.sample(false, .0001, 12345)
//val datagood = datanoheader.filter(line => line.split(",").length == 27)
val datagood = sample.filter(line => line.split(",").length == 27)
//val databad = datanoheader.filter(line => line.split(",").length != 27)
//var s1 = datagood.first.trim.split(",")

def add_time(row: Array[String]) = {
    val num_time = row(4).toDouble + row(5).toDouble/60 + row(6).toDouble/3600
    row.clone :+ num_time.toString
}

val data_with_time = datagood.map(_.trim.split(",")).map(add_time)
//s1 = add_time(s1)
//val time = data_with_time.map(row => row(27).toDouble).sample(false, .1, 12345)
//val ibyt = data_with_time.map(row => row(17).toDouble).sample(false, .1, 12345)
//val ipkt = data_with_time.map(row => row(16).toDouble).sample(false, .1, 12345)
//val time_cuts = quantiles(quant, compute_ecdf(time))
//val ibyt_cuts = quantiles(quant, compute_ecdf(ibyt))
//val ipkt_cuts = quantiles(quant, compute_ecdf(ipkt))
//val ibyt_cuts : Array[Double] = Array(0, 51.0, 71.0, 99.0, 137.0, 205.0, 305.0, 615.0, 1365.0, 4248.0)
//val ipkt_cuts : Array[Double] = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 13.0, 275586.0)
//val ipkt_cuts : Array[Double] = Array(0.0, 1, 3, 7)
//val time_cuts : Array[Double] = Array(0.0, 2.143333333333333, 4.514444444444444, 7.013055555555556, 9.296111111111111, 11.509444444444444, 13.846111111111112, 16.302777777777777, 18.907777777777778, 21.547222222222224)


def bin_ibyt_ipkt_time(row: Array[String], 
                       ibyt_cuts: Array[Double], 
                       ipkt_cuts: Array[Double],
                       time_cuts: Array[Double]) = {
    val time = row(27).toDouble
    val ibyt = row(17).toDouble
    val ipkt = row(16).toDouble
    var time_bin = 0
    var ibyt_bin = 0
    var ipkt_bin = 0
    for (cut <- ibyt_cuts){
        if (ibyt > cut) { ibyt_bin = ibyt_bin+1 }
    }
    for (cut <- ipkt_cuts){
        if (ipkt > cut) { ipkt_bin = ipkt_bin+1 }
    }
    for (cut <- time_cuts){
        if (time > cut) { time_bin = time_bin+1 }
    }
    row.clone :+ ibyt_bin.toString :+ ipkt_bin.toString :+ time_bin.toString
}

//s1 = bin_ibyt_ipkt_time(row = s1, ibyt_cuts, ipkt_cuts, time_cuts)
val binned_data = data_with_time.map(row => bin_ibyt_ipkt_time(row, ibyt_cuts, ipkt_cuts, time_cuts))

def adjust_port(row: Array[String]) = {
    var word_port = 111111.0
    val sip = row(8)
    val dip = row(9)
    val dport = row(10).toDouble
    val sport = row(11).toDouble
    val ipkt_bin = row(29).toDouble
    val ibyt_bin = row(28).toDouble
    val time_bin = row(30).toDouble
    var p_case = 0.0

    var ip_pair = row(9) + " " + row(8)
    if (sip < dip & sip != 0) { ip_pair = row(8) + " " + row(9) }
    
    if ((dport <= 1024 | sport<=1024) & (dport > 1024 | sport>1024) & min(dport,sport) != 0){
        p_case = 2
        word_port = min(dport, sport)
    }else if (dport >1024 & sport>1024){
        p_case = 3
        word_port = 333333
    }else if (dport == 0 & sport != 0){
        word_port = sport
        p_case = 4
    }else if (sport == 0 & dport != 0){
        word_port = dport
        p_case = 4
    }else {
        p_case = 1
        if (min(dport, sport) == 0) {word_port = max(dport, sport)
        }else { word_port = 111111}
    }
    //val word = ipkt_bin -1 + (ibyt_bin-1)*10 + (time_bin - 1)*100 + word_port*1000
    val word = word_port.toString+"_"+time_bin.toString+"_"+ibyt_bin.toString+"_"+ipkt_bin.toString
    var src_word = word
    var dest_word = word
    
    if (p_case == 2 & dport < sport){ dest_word = "-1_"+dest_word
    }else if (p_case == 2 & sport < dport){  src_word = "-1_"+src_word
    }else if (p_case == 4 & dport == 0){  src_word = "-1_"+src_word
    }else if (p_case == 4 & sport ==0){ dest_word = "-1_"+dest_word }
    
    row.clone :+ word_port.toString :+ ip_pair :+ src_word.toString :+ dest_word.toString
}

//s1 = adjust_port(s1)
val data_with_words = binned_data.map(row => adjust_port(row))


//next groupby src to get src word counts
val src_word_counts = data_with_words.map(row => (row(8) + " " + row(33), 1)).reduceByKey(_ + _)


//groupby dest to get dest word counts
val dest_word_counts = data_with_words.map(row => (row(9) + " " + row(34), 1)).reduceByKey(_ + _)

//val word_counts = sc.union(src_word_counts, dest_word_counts).map(row => Array(row._1.split(" ")(0).toString, row._1.split(" ")(1).toString, row._2).toString)
val word_counts = sc.union(src_word_counts, dest_word_counts).map(row => (row._1.split(" ")(0)+","+ row._1.split(" ")(1).toString +","+ row._2).mkString)


//word_counts_strings.persist(StorageLevel.MEMORY_AND_DISK)
//word_counts_strings.take(10)

// see about saving to local file system
word_counts.saveAsTextFile(output_file)


//val result = word_counts.map( row => (row.split(",")(0), (row.split(",")(1).toDouble.asInstanceOf[Long], row.split(",")(2).toDouble.asInstanceOf[Long]))).aggregateByKey(Map[Long,Long]())( (accum,value) => accum +(value._1 -> value._2), (accum1, accum2) => accum1 ++ accum2).map{ case (ip: String, wordMap: Map[Long,Long]) => {
//    val array = wordMap.toArray
//    (ip, array.map(_._1), array.map(_._2))
//    }}

val result = word_counts.map( row => (row.split(",")(0), (row.split(",")(1), row.split(",")(2).toDouble.asInstanceOf[Long]))).aggregateByKey(Map[String,Long]())( (accum,value) => accum +(value._1 -> value._2), (accum1, accum2) => accum1 ++ accum2).map{ case (ip: String, wordMap: Map[String,Long]) => {
    val array = wordMap.toArray
    (ip, array)
    }}

val result2 = result.map( row =>{
    val ip = row._1
    val array = row._2
    val num_words = array.length
    val x = array.map(t => t._1.toString+":"+t._2.toString).mkString(",")
    ip +","+num_words+","+x
    })

//result2.take(10)
//result2.saveAsTextFile(output_file_for_lda)

System.exit(0)
