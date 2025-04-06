// User Defined Functions
import spark.implicits._
val cols = Seq(“sno”,”name”)
val data = Seq((“1”,gowtham”),(“2”,”nandini”),(“3”,”saravana”))
val df = data.toDF(cols:_*)
df.show(false)
val Ucase = (strQuote: String) => { 
    val dt = strQuote. split (" ")
    dt.map(f=> f. substring (0, 1).toUpperCase + f.substring (1, f.length) ) .mkString (" ") 
    }

val customUDF = udf(Ucase)
df.select(col(“sno”),customUDF(col(“name”)).as(“name”)).show(false)


//___________________________________________________________________________________________________//
// Required imports
import org.apache.spark.sql.functions._
import spark.implicits._
// Sample data
val cols = Seq("sno", "name")
val data = Seq(("1", "gowtham"), ("2", "nandini"), ("3", "saravana"))
val df = data.toDF(cols: _*)
df.show(false)
// UDF 1: Capitalize each word
val toTitleCase = (str: String) => {
  str.split(" ").map(w => w.capitalize).mkString(" ")
}
val titleCaseUDF = udf(toTitleCase)
// UDF 2: Reverse the name
val reverseName = (str: String) => str.reverse
val reverseNameUDF = udf(reverseName)
// UDF 3: Name length
val nameLength = (str: String) => str.length
val nameLengthUDF = udf(nameLength)
// Apply UDFs
val enrichedDF = df.select(
  col("sno"),
  col("name"),
  titleCaseUDF(col("name")).as("title_case_name"),
  reverseNameUDF(col("name")).as("reversed_name"),
  nameLengthUDF(col("name")).as("name_length")
)
enrichedDF.show(false)
//___________________________________________________________________________________________________//



//___________________________________________________________________________________________________//
import org.apache.spark.sql.functions.udf
// 1. UDF to convert a string to Title Case
val toTitleCase = udf((s: String) => 
  s.split(" ").map(word => word.substring(0, 1).toUpperCase + word.substring(1).toLowerCase).mkString(" ")
)
// 2. UDF to reverse a string
val reverseString = udf((s: String) => s.reverse)
// 3. UDF to calculate the length of a string
val stringLength = udf((s: String) => s.length)
// 4. UDF to convert a string to lower case
val toLowerCase = udf((s: String) => s.toLowerCase)
// 5. UDF to convert a string to upper case
val toUpperCase = udf((s: String) => s.toUpperCase)
// 6. UDF to trim whitespace from both ends of a string
val trimString = udf((s: String) => s.trim)
//___________________________________________________________________________________________________//
