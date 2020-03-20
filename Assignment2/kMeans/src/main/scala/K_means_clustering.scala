import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, parentId: Option[Int], score: Int, domain: Option[String]) extends Serializable

/** The main class */
object Assignment2 extends Assignment2 {

    @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Assignment2")
    @transient lazy val sc: SparkContext = new SparkContext(conf)

    //sc.setLogLevel("WARN")

    /** Main function */
    def main(args: Array[String]): Unit = {
        val startTime = System.currentTimeMillis()

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

        val endTime = System.currentTimeMillis()
        println("Running time: " + (endTime - startTime) + " ms")
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
    def kmeansEta = 20.0D

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
                domain = if (arr.length >= 5) Some(arr(4).intern()) else None)
            /*      Posting(postingType =    arr(0).toInt,
                          id =             arr(1).toInt,
                          acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
                          parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
                          score =          arr(4).toInt,
                          domain =           if (arr.length >= 6) Some(arr(5).intern()) else None)*/
        })


    /** Group the questions and answers together */
    def groupedPostings(rawLines: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
        // Filter the questions and answers separately
        // Prepare them for a join operation by extracting the QID value in the first element of a tuple.
        val questions = rawLines
            .filter( x => x.postingType == 1)
            .map(x => (x.id, x))

        val answers = rawLines
            .filter(x => (x.postingType == 2 && x.parentId.isDefined))
            .map(x => (x.parentId.get, x))

        questions.join(answers).groupByKey()
    }


    /** Compute the maximum score for each posting */
    def scoredPostings(groupedLines: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {
        //ToDo
        def highestScore(posts: Iterable[Posting]): Int = {
            posts.maxBy(p => p.score).score
        }
        groupedLines.flatMap(_._2).groupByKey().mapValues(v => highestScore(v))
    }



    /** Compute the vectors for the kmeans */
    def vectorPostings(scoredLines: RDD[(Posting, Int)]): RDD[(Int, Int)] = {
        //ToDo
        def findIndex(domain: String): Option[Int] = {
            val idx = Domains.indexOf(domain)
            if (idx >= 0) Some(idx) else None
        }

        scoredLines.map(line => (findIndex(line._1.domain.get).get * DomainSpread, line._2))
    }


    // k means

    /** Main kmeans computation */
    @tailrec final def kmeans(centroids: Array[(Int, Int)], vectors: RDD[(Int, Int)], iteration: Int): Array[(Int, Int)] = {
        //ToDo

        if (iteration >= kmeansMaxIterations) {
            // base case, return and stop the recursion
            centroids
        } else {
            val clusteredVectors = vectors.map(vec => (findClosest(vec, centroids), vec))
            // calculate the new centroids
            val newCentroids = centroids.clone()

            clusteredVectors.groupByKey.mapValues(v => averageVectors(v)).collect()
                .foreach({ case (idx, p) => newCentroids.update(idx, p) })

            // compute the errors
            val distance = euclideanDistance(centroids, newCentroids)
            val isConverged = converged(distance)
            // info message
            println("iteration: "+iteration+" distance: "+BigDecimal(distance))
            println("centroids size: " + centroids.size)
            print("centroids are:")
            centroids.foreach(print)
            print("\n")
            // return means from the function
            if (isConverged == true) {
                newCentroids
            } else {
                kmeans(newCentroids, vectors, iteration + 1)
            }
        }
    }


    final def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]):
    RDD[((Int, Int), Int, Int, Int)] = {
        //ToDo
        val clusteredVectors = vectors.map(vec => (means(findClosest(vec, means)), vec))
        // results is (centroid, size of cluster, median score, average score)
        val results = clusteredVectors.groupByKey().map(cluster => (cluster._1, cluster._2.size, computeMedian(cluster._2), cluster._1._2))
        results
    }

    //  Displaying results:

    def printResults(results: RDD[((Int, Int), Int, Int, Int)]) = {
        results.collect().foreach(res => println("centroid: " + res._1 + " size: " + res._2 + " median score: " + res._3 + " average score: " + res._4))
    }


    //  Kmeans utilities (Just some cases, you can implement your own utilities.)

    /** Sample vectors */
    def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {
        // Choose k vectors as initial centroids
        vectors.takeSample(false, kmeansKernels)
    }


    /** Decide whether the kmeans clustering converged */
    def converged(distance: Double) = distance < kmeansEta


    /** Return the euclidean distance between two points */
    def euclideanDistancePointPoint(v1: (Int, Int), v2: (Int, Int)): Double = {
        val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
        val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
        part1 + part2
    }

    /** Return the euclidean distance between two points */
    def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
        assert(a1.length == a2.length)
        var sum = 0d
        var idx = 0
        while (idx < a1.length) {
            sum += euclideanDistancePointPoint(a1(idx), a2(idx))
            idx += 1
        }
        sum
    }


    /** Return the closest point */
    def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
        var bestIndex = 0
        var closest = Double.PositiveInfinity
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
    def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
        val iter = ps.iterator
        var count = 0
        var comp1: Long = 0
        var comp2: Long = 0
        while (iter.hasNext) {
            val item = iter.next
            comp1 += item._1
            comp2 += item._2
            count += 1
        }
        ((comp1 / count).toInt, (comp2 / count).toInt)
    }


    def computeMedian(a: Iterable[(Int, Int)]) = {
        val s = a.map(x => x._2).toArray
        val length = s.length
        val (lower, upper) = s.sortWith(_ < _).splitAt(length / 2)
        if (length % 2 == 0) (lower.last + upper.head) / 2 else upper.head
    }

}