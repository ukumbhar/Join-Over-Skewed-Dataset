import org.apache.log4j.{Logger,Level}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType};
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object SkewedJoin {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[2]").setAppName("DS503 Final Project")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val c = 6
    val schema_customers = StructType(Seq(StructField("ID", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Age", IntegerType, true),
      StructField("Gender", StringType, true),
      StructField("CountryCode", IntegerType, true),
      StructField("Salary", DoubleType, true)
    ))

    val customers = sqlContext.read
      .format("csv")
      .schema(schema_customers)
      .option("header","true")
      .option("delimiter", ",")
      .load("/Users/ukumbhar/IdeaProjects/BigDataProject/Customers.csv");

    customers.show(2)

    val schema_transactions = StructType(Seq(StructField("TransID", IntegerType, true),
      StructField("CustID", IntegerType, true),
      StructField("TransTotal", DoubleType, true),
      StructField("TransNumItems", IntegerType, true),
      StructField("TransDesc", StringType, true)
    ))

    val transactions = sqlContext.read
      .format("csv")
      .schema(schema_transactions)
      .option("header","true")
      .option("delimiter", ",")
      .load("/Users/ukumbhar/IdeaProjects/BigDataProject/Transactions.csv");

    transactions.show(2)

    var sampled_txn = transactions.sample(0.1)
    sampled_txn.show(30)

    var grouped = sampled_txn.groupBy("CustID").count().sort(desc("count"))
    grouped.show(50)

    var skewed_keys = List(742,5)

    var skewed_custs = customers.filter(col("ID").isin(skewed_keys:_*))
    skewed_custs.persist() //to cast skewed customers data in memory and broadcast to all mappers.
    skewed_custs.show(10)

    var non_skewed_custs = customers.filter(!col("ID").isin(skewed_keys:_*))
    non_skewed_custs.show(10)

    val t1 = System.nanoTime()
    //Re-partition join over all transactions with non-skewed customer records
    var non_skewed_join = transactions.as('txn).join(non_skewed_custs.as('cust),col("txn.CustID")===col("cust.ID"))
    non_skewed_join.rdd.saveAsTextFile("/Users/ukumbhar/Desktop/non-skewed")
    println("Re-partition join result:")
    non_skewed_join.show(50)
    val duration = (System.nanoTime() - t1) / 1e9d
    println("Runtime for Re-Partion join",duration)

    val t2 = System.nanoTime()
    //Broadcast join of all transactions with skewed customer records
    var skewed_join = transactions.as('txn).join(skewed_custs.as('cust),col("txn.CustID")===col("cust.ID"))
    skewed_join.rdd.saveAsTextFile("/Users/ukumbhar/Desktop/skewed")
    println("Broadcast join result")
    skewed_join.show(50)
    val duration2 = (System.nanoTime() - t2) / 1e9d
    println("Runtime for Broadcast join",duration2)

    println("Join Done!!!")
  }
}

