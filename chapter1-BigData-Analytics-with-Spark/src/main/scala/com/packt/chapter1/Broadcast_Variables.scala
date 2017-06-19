import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage._

object Broadcast_Variables{

  def main(args:Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("spark://master:7077").setAppName("Broadcast_Variables")
    val sc = new SparkContext(conf)

    val broadCastedTemperatures = sc.broadcast(Map("KOCHI" ->22,"BNGLR" -> 22,"HYD" -> 24, "MUMBAI" -> 21, "DELHI" -> 17,"NOIDA" -> 19, "SIMLA" -> 9))
	val inputRdd = sc.parallelize(Array("BNGLR",20), ("BNGLR",16),("KOCHI",-999),("SIMLA",-999), ("DELHI",19, ("DELHI",-999),("MUMBAI",27), ("MUMBAI",-999), ("HYD",19), ("HYD",25),("NOIDA",-999)))
	val replacedRdd = inputRdd.map{
		case(location, temperature) =>
		val standardTemperatures = broadCastedTemperatures.value
		if(temperature == -999 && standardTemperatures.get(location) !=None) 
		    (location, standardTemperatures.get(location).get) 
		else if(temperature != -999) 
		    (location, temperature )
		}
	val locationsWithMaxTemperatures =replacedRdd.reduceByKey{
		(temp1,temp2) => if (temp1 > temp2) temp1 else temp2
		}
  }

}
