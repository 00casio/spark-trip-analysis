import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Row}

object ReturnTrips {
  def compute(trips: Dataset[Row], dist: Double, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    val earthRadius = 6371.0
    val makeDistExpr = (lat1: Column, lon1: Column, lat2: Column, lon2: Column) => {
      val dLat = toRadians(abs(lat2 - lat1))
      val dLon = toRadians(abs(lon2 - lon1))
      val hav = pow(sin(dLat * 0.5), 2) + pow(sin(dLon * 0.5), 2) * cos(toRadians(lat1)) * cos(toRadians(lat2))
      abs(lit(earthRadius * 2) * asin(sqrt(hav)))
    }
    val tripsA = trips.alias("a")
    val tripsB = trips.alias("b")
    val joinedTrips = tripsA.crossJoin(tripsB)
      .filter(
        makeDistExpr($"a.dropoff_lat", $"a.dropoff_lon", $"b.pickup_lat", $"b.pickup_lon") < dist &&
        makeDistExpr($"b.dropoff_lat", $"b.dropoff_lon", $"a.pickup_lat", $"a.pickup_lon") < dist &&
        $"a.dropoff_time" < $"b.pickup_time" &&
        $"a.dropoff_time" + expr("INTERVAL 8 HOURS") > $"b.pickup_time"
      )

    val result = joinedTrips.select($"a.*", $"b.*")

    result
  }
}