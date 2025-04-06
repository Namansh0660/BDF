var a = sc.textFile("/home/ponny/Desktop/test").flatMap(line =? line.split(" ")).map(word => (word,1))
var b = a.reduceByKey(_+_);
b.collect

//___________________________________________________________________________________________________//
// Word Count with K-th word in top-N and average frequency
val file = sc.textFile("/home/ponny/Desktop/test")
// Clean and tokenize
val words = file
  .flatMap(line => line.split("\\s+"))
  .map(_.replaceAll("[^a-zA-Z]", "").toLowerCase())
  .filter(_.nonEmpty)
// Count word frequencies
val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
// Collect and sort
val wordCountList = wordCounts.collect().toList
val sorted = wordCountList.sortBy(-_._2)
// Parameters
val N = 4  // Top-N most frequent
val K = 2  // K-th word in top-N
// Extract top-N
val topN = sorted.take(N)
if (K <= topN.length) {
  val kthWord = topN(K - 1)
  println(s"$K-th most frequent word in top $N: '${kthWord._1}' with frequency ${kthWord._2}")
} else {
  println(s"K ($K) is larger than the number of words in top $N")
}
// Average frequency
val totalCount = sorted.map(_._2).sum
val average = totalCount.toDouble / sorted.size
println(f"Average word frequency: $average%.2f")
//___________________________________________________________________________________________________//
