import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object Assignment2 extends Assignment2 {

    @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Assignment2")
    @transient lazy val sc: SparkContext = new SparkContext(conf)

    //sc.setLogLevel("WARN")

    /** Main function */
    def main(args: Array[String]): Unit = {
        val Output = "../Task2_data/output"
        val lines = sc.textFile("../Task2_data/QA_data/QA_data.csv")
        val raw = rawPostings(lines)
        val grouped = groupedPostings(raw)
        val scored = scoredPostings(grouped)
        val vectors = vectorPostings(scored).cache()
        // initial centroids
        val sampledVectors = sampleVectors(vectors)
        // k-means algorithm
        val means = kmeans(sampledVectors, vectors, 0)
        // format the results, printout and save
        val results = clusterResults(means, vectors)
        printResults(results)
        results.saveAsTextFile(Output)
    }
}


/** The parsing and kmeans methods */
class Assignment2 extends Serializable {

    /** Languages */
    val Domains =
        List(
            "Machine-Learning", "Compute-Science", "Algorithm", "Big-Data", "Data-Analysis", "Security", "Silicon Valley", "Computer-Systems",
            "Deep-learning", "Internet-Service-Providers", "Programming-Language", "Cloud-services", "Software-Engineering", "Embedded-System", "Architecture")


    /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
    def DomainSpread = 50000

    assert(DomainSpread > 0)

    /** K-means parameter: Number of clusters */
    def kmeansKernels = 45

    /** K-means parameter: Convergence criteria, if changes of all centriods < kmeansEta, stop */
    def kmeansEta: BigDecimal = 20.0D

    /** K-means parameter: Maximum iterations */
    def kmeansMaxIterations = 120

    // Data Processing
    /** Load postings from the given file */
    def rawPostings(lines: RDD[String]): RDD[Posting] =
        lines.map(line => {
            val arr = line.split(",")
            Posting(postingType = arr(0).toInt,
                id = arr(1).toInt,
                parentId = if (arr(2) == "") None else Some(arr(2).toInt),
                score = arr(3).toInt,
                tags = if (arr.length >= 5) Some(arr(4).intern()) else None)
            /*      Posting(postingType =    arr(0).toInt,
                          id =             arr(1).toInt,
                          acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
                          parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
                          score =          arr(4).toInt,
                          tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)*/
        })


    /** Group the questions and answers together */
    def groupedPostings(rawLines: RDD[Posting]): RDD[(Int, Iterable[Posting])] = {
        // Filter the questions and answers separately
        // Prepare them for a join operation by extracting the QID value in the first element of a tuple.
        rawLines.groupBy(x => if (x.postingType == 1) x.id else x.parentId match {
            case Some(s) => s
        })
    }


    /** Compute the maximum score for each posting */
    def scoredPostings(groupedLines: RDD[(Int, Iterable[Posting])]): RDD[(Int, Int)] = {
        //ToDo
        groupedLines.mapValues(v => v.maxBy(x => x.score).score)
    }


    /** Compute the vectors for the kmeans */
    def vectorPostings(scoredLines: RDD[(Int, Int)]): RDD[(BigInt, Int)] = {
        //ToDo
        scoredLines.map(pair => (BigInt(pair._1) * BigInt(DomainSpread), pair._2))
    }


    // k means

    /** Main kmeans computation */
    @tailrec final def kmeans(centroids: Array[(BigInt, Int)], vectors: RDD[(BigInt, Int)], iteration: Int): Array[(BigInt, Int)] = {
        //ToDo

        if (iteration >= kmeansMaxIterations) {
            // base case, return and stop the recursion
            centroids
        } else {
            val clusteredVectors = vectors.map(vec => (centroids(findClosest(vec, centroids)), vec))
            // calculate the new centroids
            val newClusters = clusteredVectors.groupByKey()
                .map(group => (averageVectors(group._2), group._2))
            val newCentroids = newClusters.keys
            // compute the errors
            val distance = newClusters
                .map(cluster => (cluster._1, euclideanDistanceArrayPoint(cluster._2.toArray, cluster._1)))
                .values
                .sum()
            val isConverged = converged(distance)
            // return means from the function
            if (isConverged == true) {
                newCentroids.collect()
            } else {
                kmeans(newCentroids.collect(), vectors, iteration + 1)
            }
        }
    }


    final def clusterResults(means: Array[(BigInt, Int)], vectors: RDD[(BigInt, Int)]):
    RDD[((BigInt, Int), Int, Int, Int)] = {
        //ToDo
        val clusteredVectors = vectors.map(vec => (means(findClosest(vec, means)), vec))
        // results is (centroid, size of cluster, median score, average score)
        val results = clusteredVectors.groupByKey().map(cluster => (cluster._1, cluster._2.size, computeMedian(cluster._2), cluster._1._2))
        results
    }

    //  Displaying results:

    def printResults(results: RDD[((BigInt, Int), Int, Int, Int)]) = {
        results.foreach(res => println("centroid: " + res._1 + " size: " + res._2 + " median score: " + res._3 + " average score: " + res._4))
    }


    //  Kmeans utilities (Just some cases, you can implement your own utilities.)

    /** Sample vectors */
    def sampleVectors(vectors: RDD[(BigInt, Int)]): Array[(BigInt, Int)] = {
        // Choose k vectors as initial centroids
        val sampledVectors = vectors.takeSample(false, kmeansKernels)
        sampledVectors
    }


    /** Decide whether the kmeans clustering converged */
    def converged(distance: BigDecimal) = distance < kmeansEta


    /** Return the euclidean distance between two points */
    def euclideanDistancePointPoint(v1: (BigInt, Int), v2: (BigInt, Int)): BigDecimal = {
        val part1 = (BigDecimal) (v1._1 - v2._1) * (BigDecimal) (v1._1 - v2._1)
        val part2 = (BigDecimal) (v1._2 - v2._2) * (BigDecimal) (v1._2 - v2._2)
        part1 + part2
    }

    /** Return the euclidean distance between two points */
    def euclideanDistanceArrayArray(a1: Array[(BigInt, Int)], a2: Array[(BigInt, Int)]): BigDecimal = {
        assert(a1.length == a2.length)
        var sum: BigDecimal = 0d
        var idx = 0
        while (idx < a1.length) {
            sum += euclideanDistancePointPoint(a1(idx), a2(idx))
            idx += 1
        }
        sum
    }


    /** Return the euclidean distance between two points */
    def euclideanDistanceArrayPoint(a1: Array[(BigInt, Int)], v2: (BigInt, Int)): BigDecimal = {
        var sum: BigDecimal = 0d
        var idx = 0
        while (idx < a1.length) {
            sum += euclideanDistancePointPoint(a1(idx), v2)
            idx += 1
        }
        sum
    }


    /** Return the closest point */
    def findClosest(p: (BigInt, Int), centers: Array[(BigInt, Int)]): Int = {
        var bestIndex = 0
        var closest: BigDecimal = null
        for (i <- 0 until centers.length) {
            val tempDist = euclideanDistancePointPoint(p, centers(i))
            if (closest == null || tempDist < closest) {
                closest = tempDist
                bestIndex = i
            }
        }
        bestIndex
    }


    /** Average the vectors */
    def averageVectors(ps: Iterable[(BigInt, Int)]): (BigInt, Int) = {
        val iter = ps.iterator
        var count = 0
        var comp1: BigInt = 0
        var comp2: Int = 0
        while (iter.hasNext) {
            val item = iter.next
            comp1 += item._1
            comp2 += item._2
            count += 1
        }
        ((comp1 / count), (comp2 / count))
    }


    def computeMedian(a: Iterable[(BigInt, Int)]) = {
        val s = a.map(x => x._2).toArray
        val length = s.length
        val (lower, upper) = s.sortWith(_ < _).splitAt(length / 2)
        if (length % 2 == 0) (lower.last + upper.head) / 2 else upper.head
    }

}