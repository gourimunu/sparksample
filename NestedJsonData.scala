//sc spark context
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val people = sqlContext.jsonFile("people.json")

people.printSchema()

people.registerTempTable("people")

 val add = sqlContext.sql("select name, address.city from people").show()

 val add = sqlContext.sql("select name, address.city, Data.Data2.data2 from people").show()

 val add = sqlContext.sql("select name, address.city, Data.Data2.data2 from people").toDF()

 add.printSchema()


//Geo data2

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val people = sqlContext.jsonFile("GeoJson.json")
val geo = sqlContext.jsonFile("GeoJson.json")
geo.printSchema()
geo.printSchema()
geo.registerTempTable("geo")
scala> val add = sqlContext.sql("select * from geo").show()





