import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math.min

object CommonWords {

    def main(args: Array[String]) {
        val startTime = System.currentTimeMillis()

        // 3 input file paths
        val InputFile1 = "C:/Repository/CS4225-Data-System/Assignment2/Task1_data/task1-input1.txt"
        val InputFile2 = "C:/Repository/CS4225-Data-System/Assignment2/Task1_data/task1-input2.txt"
        val StopWordsFile = "C:/Repository/CS4225-Data-System/Assignment2/Task1_data/stopwords.txt"
        val Output = "C:/Repository/CS4225-Data-System/Assignment2/Task1_data/output"
        val Delimiter = "[ \t\n\r\f]"

        // spark basic set up
        val conf = new SparkConf().setAppName("commonWords").setMaster("local")
        val sc = new SparkContext(conf)

        // process stop words and cache them
        val stopWordsRdd = sc.textFile(StopWordsFile)
            .flatMap(line => line.split(Delimiter)
            .map(word => word.toLowerCase))
            .cache()
        val stopWordsSet = stopWordsRdd.collect.toSet

        // process the first input file
        val textFile1Rdd = sc.textFile(InputFile1)
        val counts1 = textFile1Rdd.flatMap(line => line.split(Delimiter))
            .filter(word => ! stopWordsSet.contains(word.toLowerCase()))
            .map(word => (word, 1))
            .reduceByKey(_ + _)
            .cache()

        // process the second input file
        val textFile2Rdd = sc.textFile(InputFile2)
        val counts2 = textFile2Rdd.flatMap(line => line.split(Delimiter))
            .filter(word => (! stopWordsSet.contains(word.toLowerCase())) && (word != ""))
            .map(word => (word, 1))
            .reduceByKey(_ + _)
            .cache()

        // find common count
        val result = counts1.join(counts2)
            .map(v => (v._1, min(v._2._1, v._2._2)))
            .sortBy(pair => pair._2, false)
            .take(15)

        sc.parallelize(result).saveAsTextFile(Output)

        val endTime = System.currentTimeMillis()
        println("Execution time in Spark: " + (endTime - startTime) + " ms")

    }
}
