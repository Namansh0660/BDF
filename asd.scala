var a = sc.textFile("/home/ponny/Desktop/test").flatMap(line =? line.split(" ")).map(word => (word,1))

var b = a.reduceByKey(_+_);

b.collect