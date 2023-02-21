# Lab 2 Report

The members of Group 29 are:
- Srinath Jayaraman - 5049903
- Juan Molano - 5239540

## Usage

The spark application is compiled and assembled inside sbt. 

The app has the following default parameters:
1. Input file path: "s3://abs-tudelft-sbd20/netherlands.orc"
2. Output file name: "s3://sbdlab2group29/breweries_nl_all"
3. Number of partitions: 20 (default)

But they can be modified with the following input parameters:

- 'i' <-- Input file path
- 'o' <-- Output file path 
- 'p' <-- Number of partitions

While submitting the app on AWS EMR, we provide the appropriate arguments as shown in the image below:

![arg image](https://github.com/abs-tudelft-sbd20/lab-2-group-29/blob/Robust3/Arguments%20passed%20to%20AWS.png)

## Approach
**NOTE: We were told to use AWS Educate. However, we ran into issues trying to allocate compute optimised instances (c5.\*) since there were some permission errors that we suspect are a result of limitations placed on AWS Educate. We ended up using the General Purpose m5 instances.**

The run-times we are required to aim for are given below for reference (taken from the lab guide):
- Netherlands - 1.5 min - aim for less than 5 minutes
- United States - 6.9 min - aim for less than 15 minutes
- Europe - 22 min - aim for less than 1 hour
- Planet - 56 min - aim for less than 2 hours

### Iteration 0
Iteration 0 involved using relations from OSM (OpenStreetMap). We fetch nodes, ways, and relations as shown in the code block below. We first tried to use the JTS library to create polygons but we kept running into issues with it. We tried to implement our own code with JTS for nearly 2 days, but ultimately switched to using a different implementation of it that was provided by the TA's.

```scala
val nodes = df
      .filter('type === "node")
      .select(
        'id.as("node_id"),
        'tags,
        $"lat".cast("Double"),
        $"lon".cast("Double")
      )

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
      .select($"way_id", $"node", struct($"lat", $"lon").as("point"))
      .groupBy('way_id)
      .agg(
        array_sort(
          collect_list(
            struct($"node.pos".as("pos"), $"node.id".as("node_id"), $"point")
          )
        ).as("way")
      )
      
      
    
    val relations = df.filter('type === "relation")
    .filter(
        ($"tags".getItem("type").equalTo("boundary") &&
          $"tags".getItem("boundary").equalTo("administrative")
          && $"tags".getItem("admin_level").equalTo("8") 
          )
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
      .select(
        'relation_id,
        $"city",
        $"relation",
        posexplode($"way.point")
      ).sort($"relation_id", $"relation.pos", $"pos".asc)
      .select(
        'relation_id,
        $"city",
        $"relation.pos".as("way_pos"),
        $"relation.ref".as("line"),
        $"pos".as("node_pos"),
        $"col".as("point")
      ).groupBy('relation_id, 'city, 'way_pos)
      .agg(
        array_sort(
          collect_list(
            $"point"
          )
        ).as("lines")
      )
      .groupBy('relation_id, 'city)
      .agg(
        array_sort(
          collect_list(
            $"lines"
          )
        ).as("lines")
      ).select(
        $"relation_id",
        $"city",
        createGeometry(col("lines")).as("polygon")
      ).select(
        $"relation_id",
        $"city",
        explode($"polygon").as("polygon")
      )
```
This is the first part of our approach and it takes our program to robustness Level 3 as outlined in the guide to Lab 2. 

The run time on our local machine for the full Netherlands dataset was 818 seconds or 13.6 minutes, as shown in the image and table below. 

![iteration 0 image](https://github.com/abs-tudelft-sbd20/lab-2-group-29/blob/Robust3/Local%20runtime%20for%20full%20netherlands.png)

| | |
|---|---|
| System                  | Core i7-8565U, 4 cores, 8 threads, base clock 1.80 GHz |
| Dataset                 | Netherlands | 
| Run time <br>(hh:mm:ss) | 00:13:06 | 

As you can see in the image below, USA and Netherlands ran in quick time (22 minutes and 2 minutes respectively). This was because we were running it on our own AWS cluster, **not** on AWS Educate, which is why we were able to use the more powerful c5.\* instances. Ultimately, we stopped doing that since it cost us real money, and we are just grad students!

![Run time for Europe and the planet](https://github.com/abs-tudelft-sbd20/lab-2-group-29/blob/Robust3/Run%20time%20for%20Europe%20and%20the%20planet.png)

Since we are asked to aim for less than 15 minutes for the USA data-set, clearly there were some improvements needed to decrease run time. We used an external sort along with the ```array_sort``` command in the lab guide, but we quickly ran into issues when running it on the cloud. It worked for a smaller data-set like "zuid holland.orc" but we ran into memory issues for larger datasets. The run time for Europe and the planet was **several hours** for each of the 2 data-sets. We had to keep killing the step when running it on AWS for Europe and the planet, seen in the image below:

Our take-away was that the sort was the issue here, seen below as we **originally** used it:
```scala
.sort($"relation_id", $"relation.pos".asc)
```
We used the function given by the TA's because we kept running into trouble with our own implementation and found the ```createGeometry``` function easier to understand and implement.
```scala
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
```

### Iteration 1
We kept the partitions code from Lab 1. We kept the code because using partitions gave us better control over parallelization in spark. We gained 9 minutes of run-time when we used 40 partitions for the USA dataset instead of the default 20.  

We changed the sorting algorithm since we stopped using the external sort because it was causing memory issues with the executors and it was throwing some warnings as well. The executors failed due to memory issues as shown here:

![USA iteration 0 memory issues](https://github.com/abs-tudelft-sbd20/lab-2-group-29/blob/Robust3/USA%20iteration%200.png)

We fixed the issue with the external sort by using **only** the recommended ```array_sort```from earlier. The run times for Netherlands and USA are shown below:

![usa and netherlands iteration 1](https://github.com/abs-tudelft-sbd20/lab-2-group-29/blob/Robust3/usa%20and%20netherlands%20iteration%201.png)

### Iteration 2
We had a session with the TA's on Tuesday, 13th October, where they recommended using h3 indexes. The reason is that integer processing is much faster than complex structures, and so h3 indexes would help speed up our run times. The implementation is given below:

```scala
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
```

We also included it in our relations code:

```scala
.withColumn("polygon", createGeometry(col("lines")).as("polygon"))
      .withColumn("polygon", explode($"polygon"))
      .withColumn("h3index", polygonToH3(col("polygon"), lit(res)))
      .withColumn("h3index", explode($"h3index"))
      .repartition(partitions)
```
The run times for USA and Netherlands for iteration 2 are given below:

![Netherlands and USA iteration 2](https://github.com/abs-tudelft-sbd20/lab-2-group-29/blob/Robust3/Netherlands%20and%20USA%20iteration%202.png)

However, we are still facing issues with Europe and the planet running for several hours. Here is a screenshot of the run time from the logs for Europe. 8215 seconds ~ 2.2 hours:

![Run time for Europe before cancellation](https://github.com/abs-tudelft-sbd20/lab-2-group-29/blob/Robust3/Run%20time%20for%20Europe%20before%20cancellation.png)


## Summary of application-level improvements

- Using only array_sort and not external sort
- Using dataframe API instead of dataset API (learned from Lab 1)
- Using partitions to have better control of parallelization
- Using h3 indexes integer processing is much faster than complex structures 
 
## Cluster-level improvements

- Using *m5.2xlarge* instances instead of *m4.large* since they have more vCPU and memory
- Due to not enough capacity in spot instance pool, we used on-demand instances
- Increased the number of core nodes from the default 2 to 5 for better parallel processing
- Increased EBS storage on our clusters

## Conclusion

What we took away from this lab:<br/>
- Finding geographical boundaries is **much** harder than we anticipated, but also much more useful and intuitive<br/>
- *c5.xlarge* instances, as they are compute optimized, allowed us to perform better even for unefficient implementations. Nevertheless, to obtain this computing efficiency comes an extra cost and is cheaper to optimize the code itself.<br/>
- This Lab required careful reading of the SPARK history server to understand the bottlenecks<br/>
- Fine-tuning our parameters on AWS took more time and thought than we imagined (it is not simply a case of adding more vCPU's)<br/>
- Fine-tuning our code to run more efficiently can be a nightmare in big data problems<br/>
- There are multiple optimization techniques to keep in mind - different types of sorting, partitioning, caching, indexing, etc.
- Parallel processing is not perfect and it takes a while to arrive at the optimal combination of instances<br/>
