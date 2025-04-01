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