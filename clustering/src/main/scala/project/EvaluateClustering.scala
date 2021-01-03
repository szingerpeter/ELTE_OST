package project

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.util

import scala.collection.JavaConversions._


class EvaluateClustering extends KeyedProcessFunction[Long, List[ClusteringResult], (List[ClusteringResult], Double)] {

    type In = List[ClusteringResult]
    type Out = (List[ClusteringResult], Double)

    private def euclidean_distance(a1: Array[Double], a2: Array[Double]): Double = {
        require(a1.size == a2.size, "array sizes should be the same")
        var sum = 0.0
        a1.zip(a2).map({ case (x, y) => scala.math.pow((x - y), 2) }).foreach( sum += _ )
        sum
    }


    override def processElement(in: In, context: KeyedProcessFunction[Long, List[ClusteringResult], Out]#Context, collector: Collector[Out]) : Unit = {

        val clustering_result: In = in

        val cluster_centers = clustering_result.map(x => {
            (x.cluster_label, x.cluster_info.getCenter)
        }).distinct

        val results = clustering_result.map(x => {
            val location_id = x.location_id
            val cluster_label = x.cluster_label
            val features = x.features

            val intra_cluster_center = cluster_centers.filter(_._1 == cluster_label).get(0)._2
            val intra_cluster_distance = euclidean_distance(features, intra_cluster_center)

            val min_outer_cluster_distance = cluster_centers.map(x => {
                euclidean_distance(x._2, features)
            }).min

            val silhouette_sample = (intra_cluster_distance - min_outer_cluster_distance) /
              Array(intra_cluster_distance, min_outer_cluster_distance).max

            silhouette_sample
        })

        collector.collect((in, results.sum / results.length))
    }
}
