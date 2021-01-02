package project

import com.yahoo.labs.samoa.instances.{Attribute, DenseInstance, Instances, InstancesHeader}
import moa.clusterers.`macro`.NonConvexCluster
import moa.core.FastVector
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.util.Collector

import java.util
import scala.collection.JavaConversions._
import scala.math.sqrt

class ProcessClustering(isDebug : Boolean = false) extends KeyedProcessFunction[Long, Map[Long, List[Measurement]], ClusteringResult] {
    
    type In = Map[Long, List[Measurement]]
    type Out = ClusteringResult
    
    lazy val state : ValueState[DenStream] = getRuntimeContext
      .getState(
          new ValueStateDescriptor[DenStream]("state", classOf[DenStream])
      )
    
    val sideOutputTag: OutputTag[List[Measurement]] = OutputTag[List[Measurement]]("Measurements")
    val inclusion_prob : Double = 0.75
    val FFT : FFT = new FFT()
    
    def generateAttributeHeader() : InstancesHeader = {
        
        val attributes = new FastVector[Attribute]
        attributes.addElement(new Attribute("Weighted Avg"))
        attributes.addElement(new Attribute("Max"))
        attributes.addElement(new Attribute("Min"))
        
        new InstancesHeader(
            new Instances("instance", attributes, 0)
        )
    }
    
    private def extractFeatures(fft: Array[(Double, Double)]) : Array[Double] = {
        Array(
            fft.map { item => item._1*item._2 }.sum / fft.length,
            fft.maxBy(_._2)._2,
            fft.minBy(_._2)._2
        )
    }
    
    def preprocess(in : In): Map[Long, DenseInstance] = {
        
        in.map { location =>
        
            val id = location._1
            val measurements = location._2
            
            // Calculating FFT representation
            var fft = FFT.fft(measurements.map(_._measurement).toArray)
            
            // Normalizing the data
            fft = fft.map { item =>
                val (freq, amp) = item
                (freq, amp / sqrt(fft.map { item => item._2*item._2 }.sum))
            }
            
            // Extract features
            val summary : Array[Double] = this.extractFeatures(fft)
    
            // Logging
            if (isDebug) {
                System.out.println("FFT")
                fft.foreach { item => System.out.print(s"${item} ") }
                System.out.println("Summary")
                summary.foreach { item => System.out.print(s"${item} ") }
            }
            
            // Create Datapoint instance
            val instance = new DenseInstance(1.0D, summary)
            instance.setDataset(generateAttributeHeader())
            (id, instance)
        }
    }
    
    def extractResults(input : Map[Long, DenseInstance], clustering : DenStream) : List[(Long, Long, NonConvexCluster)] = {
        
        // Get Clustering result based on prior points
        val result = this
          .state
          .value()
          .getClusteringResult()
          .getClustering
          .asInstanceOf[util.ArrayList[NonConvexCluster]]
        
        result.foreach { cluster =>
            System.out.println(new Out(cluster_info = cluster).clusterSummary())
        }
    
        // Assign labels based on the size of cluster overlapping area
        input.map { location =>
        
            var label : Long = -1
            var cluster_info = result(0)
            for (i <- result.indices) {
                val cluster = result(i)
                if (cluster.getInclusionProbability(location._2) > inclusion_prob) {
                    label = cluster.getId.toLong
                    cluster_info = cluster
                }
            }
            (location._1, label, cluster_info)
        }.toList
    }
    
    override def processElement(in: In, context: KeyedProcessFunction[Long, Map[Long, List[Measurement]], Out]#Context, collector: Collector[Out]) : Unit = {
        
        // TODO: save secondary output to influxdb
        // TODO: implement mahalanobis distance
        
        // Initialize clustering & Load from state
        var clustering: DenStream = state.value()
        if (clustering == null) {
            clustering = new DenStream()
            clustering.prepareForUse()
            clustering.resetLearningImpl()
            System.out.println("Creating new DenStream instance...")
        }
        
        // TODO: reset Denstream object's state
        //clustering.resetLearningImpl()
        
        // Preprocess input
        val input : Map[Long, DenseInstance] = preprocess(in)
        
        // Add data points to clustering
        input.foreach { item =>
            val instance = item._2
            clustering.trainOnInstanceImpl(instance)
        }
    
        // Update state
        this.state.update(clustering)
        
        // Extract results && collect them
        extractResults(input, clustering).foreach { item =>
          
            val location_id = item._1
            val label = item._2
            val info = item._3
            
            val measurements = in.get(location_id) match {
                case Some(value) => value
                case None => List()
            }
            
            val output : Out = new Out(location_id, label, measurements, info)
            collector.collect(output)
    
            // Put original measurements into secondary output
            context.output(sideOutputTag, measurements)
        }
    }
}
