import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.geom._
import scala.collection.mutable.WrappedArray
import org.locationtech.jts.operation.polygonize.Polygonizer
import com.uber.h3core.H3Core
import com.uber.h3core.util.GeoCoord
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object Lab2 {

  case class Breweries(
      city: String,
      breweries: Integer
  )

  def main(args: Array[String]) {
    val schema = StructType(
      Array(
        StructField("city", StringType),
        StructField("breweries", IntegerType)
      )
    )

    // Default values
    var inputFile: String = "s3://abs-tudelft-sbd20/netherlands.orc"
    var outputFile: String = "breweries"
    var partitions: Int = 20
    val res = 7

    for (value <- 0 to args.length - 1) {
      if (args(value).startsWith("i")) {
        if (value + 2 > args.length) {
          println("Enter a valid input value")
          println("Using default value: " + inputFile)
        }
        inputFile = args(value + 1)
      }
      if (args(value).startsWith("o")) {
        if (value + 2 > args.length) {
          println("Enter a valid output value")
          println("Using default value: " + outputFile)
        }
        outputFile = args(value + 1)
      }
      if (args(value).startsWith("p")) {
        if (value + 2 > args.length) {
          println("Enter a valid number of partitions")
          println("Using default value: " + partitions)
        }
        partitions = args(value + 1).toInt
      }
    }

    // Inputs print
    println("Input file path: " + inputFile)
    println("Output file name: " + outputFile)
    println("Number of partitions: " + partitions)

    val spark = SparkSession.builder
      .appName("Lab 2")
      .getOrCreate()

    // val spark = SparkSession
    //   .builder()
    //   .appName("Lab 2")
    //   .config("spark.master", "local")
    //   .getOrCreate()
    val sc = spark.sparkContext // If you need SparkContext object

    spark.withJTS

    import spark.implicits._

    // udf
    object GeoService {
      @transient lazy val geometryFactory = new GeometryFactory
    }

    val createGeometry =
      org.apache.spark.sql.functions.udf {
        (points: WrappedArray[WrappedArray[Row]]) =>
          val polygonizer = new Polygonizer
          points
            .map(
              _.map { case Row(lat: Double, lon: Double) => (lat, lon) }
                .map(a => new Coordinate(a._2, a._1))
            )
            .map(x => GeoService.geometryFactory.createLineString(x.toArray))
            .foreach(polygonizer.add)
          val collection = polygonizer.getGeometry
          (0 until collection.getNumGeometries).map(collection.getGeometryN)
      }

    object H3 extends Serializable {
      val instance = H3Core.newInstance()
    }

    val geoToH3 = org.apache.spark.sql.functions.udf {
      (latitude: Double, longitude: Double, resolution: Int) =>
        H3.instance.geoToH3(latitude, longitude, resolution)
    }

    val polygonToH3 = org.apache.spark.sql.functions.udf {
      (geometry: Geometry, resolution: Int) =>
        var points: List[GeoCoord] = List()
        var holes: List[java.util.List[GeoCoord]] = List()
        if (geometry.getGeometryType == "Polygon") {
          points = List(
            geometry
              .getCoordinates()
              .toList
              .map(coord => new GeoCoord(coord.y, coord.x)): _*
          )
        }
        H3.instance.polyfill(points, holes.asJava, resolution).toList
    }

    // Read input data
    val df = spark.read
      .format("orc")
      .load(inputFile)

    // Obtaining the dataset for the nodes
    val nodes = df
      .filter('type === "node")
      .select(
        'id.as("node_id"),
        $"lat".cast("Double"),
        $"lon".cast("Double")
      )
      .withColumn("point", struct($"lon", $"lat"))
      .repartition(partitions)

    // Obtaining the dataset for the ways
    val ways = df
      .filter('type === "way")
      .select(
        'id.as("way_id"),
        posexplode($"nds.ref")
      )
      .select(
        'way_id,
        struct('pos, 'col.as("id")).as("node")
      )
      .join(nodes, $"node.id" === 'node_id)
      .groupBy('way_id)
      .agg(
        array_sort(
          collect_list(
            struct($"node.pos".as("pos"), $"node.id".as("node_id"), $"point")
          )
        )
          .as("way")
      )

    // Finding the relations and creating its own polygon
    val relations = df
      .filter(
        ('type === "relation"
          && $"tags".getItem("type").equalTo("boundary")
          && $"tags".getItem("boundary").equalTo("administrative")
        // && $"tags".getItem("name").equalTo("Delft")
          && $"tags".getItem("admin_level").equalTo("8"))
      )
      .withColumn("city", $"tags".getItem("name"))
      .select(
        'id.as("relation_id"),
        $"city",
        posexplode($"members")
      )
      .filter(
        $"col.type".equalTo("way") &&
          $"col.role".equalTo("outer")
      )
      .select(
        'relation_id,
        $"city",
        struct('pos, $"col.ref".as("ref")).as("relation")
      )
      .join(ways, $"relation.ref" === 'way_id)
      // .sort($"relation_id", $"relation.pos".asc)
      .select(
        'relation_id,
        $"city",
        $"relation.pos".as("relation_pos"),
        $"way_id",
        posexplode($"way.point")
      )
      .groupBy('relation_id, 'city, 'relation_pos)
      .agg(
        collect_list(
          $"col"
        )
          .as("lines")
      )
      .groupBy('relation_id, 'city)
      .agg(
        array_sort(
          collect_list(
            struct($"relation_pos", $"lines")
          )
        )
          .as("lines")
      )
      .select(
        'relation_id,
        'city,
        explode($"lines.lines")
      )
      .groupBy('relation_id, 'city)
      .agg(
        collect_list(
          $"col"
        )
          .as("lines")
      )
      .withColumn("polygon", createGeometry(col("lines")).as("polygon"))
      .withColumn("polygon", explode($"polygon"))
      .withColumn("h3index", polygonToH3(col("polygon"), lit(res)))
      .withColumn("h3index", explode($"h3index"))
      .repartition(partitions)

    relations.cache

    // FILTER THE DATA FOR BREWERIES
    //  nodes
    val brewery_node = df
      .filter('type === "node")
      .filter(
        ($"tags".getItem("craft").equalTo("brewery")) ||
          ($"tags".getItem("microbrewery").equalTo("yes")) ||
          ($"tags".getItem("name").contains("brouwerij"))
      )
      .select("id", "lat", "lon")

    //  ways
    val brewery_way = df
      .filter('type === "way")
      .filter(
        ($"tags".getItem("craft").equalTo("brewery")) ||
          ($"tags".getItem("microbrewery").equalTo("yes")) ||
          ($"tags".getItem("name").contains("brouwerij"))
      )
      .select(
        'id,
        explode($"nds.ref")
      )
      .join(nodes, $"col" === 'node_id)
      .createOrReplaceTempView("brewery_way")

    val brewery_ways = spark
      .sql(
        "SELECT id, avg(lat) as lat, avg(lon) as lon FROM brewery_way group by id"
      )
    //  relations
    val brewery_relation = df
      .filter('type === "relation")
      .filter(
        ($"tags".getItem("craft").equalTo("brewery")) ||
          ($"tags".getItem("microbrewery").equalTo("yes")) ||
          ($"tags".getItem("name").contains("brouwerij"))
      )
      .select(
        'id,
        explode($"members")
      )
      .filter(
        $"col.type".equalTo("node")
      )
      .select(
        'id,
        $"col.ref".as("ref").as("node")
      )
      .join(nodes, $"node" === 'node_id)
      .createOrReplaceTempView("brewery_relation")

    val brewery_relations = spark
      .sql(
        "SELECT id, avg(lat) as lat, avg(lon) as lon FROM brewery_relation group by id"
      )

    //  Joining all the breweries found
    val breweries = brewery_node
      .repartition(partitions)
      .union(brewery_ways.repartition(partitions))
      .union(brewery_relations.repartition(partitions))
      // .withColumn("point", st_makePoint(col("lat"), col("lon")))
      .withColumn("h3index", geoToH3(col("lon"), col("lat"), lit(res)))

    // Finding its belonging city
    val total = breweries
      .repartition(partitions)
      .join(relations.repartition(partitions), "h3index")
    // .where(st_contains($"polygon", $"point"))

    total.createOrReplaceTempView("cities")

    // Getting the final count
    val finalCount = spark
      .sql(
        "SELECT city, count(*) as breweries_number FROM cities GROUP BY city ORDER BY breweries_number desc"
      )
      .select(
        $"city" as "city",
        $"breweries_number".as("breweries").cast("Int")
      )
      .as[Breweries]

    // Writing the answer
    finalCount.write.mode("overwrite").orc(outputFile)

    spark.stop
  }

}
