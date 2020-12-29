package project

import com.yahoo.labs.samoa.instances.{DenseInstance, Instance}
import moa.core.Measurement
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.util.Collector

class ProcessClustering[In, Out] extends KeyedProcessFunction[DenStream, In, Out] {
    
    lazy val state : ValueState[DenStream] = getRuntimeContext()
      .getState(
          new ValueStateDescriptor[DenStream]("state", classOf[DenStream])
      )
    
    override def processElement(in : In, context : KeyedProcessFunction[DenStream, In, Out]#Context, collector : Collector[Out]) : Unit = {
        
        // TODO: convert elements with fft
        // TODO: create DenPoint instance
        // TODO: feed to Denstream
        // TODO: save & show output to secondary sink
        // TODO: extract results
        
        // Setting up an instance
        val instance = {
            val n_measurements = 5
            val values = new Array[Double](n_measurements + 1)
            for (i <- 0 until n_measurements) {
                values(i) = i
            }
            new DenseInstance(1.0D, values)
        }
        
        var clustering : DenStream = state.value()
        clustering.trainOnInstanceImpl(instance)
        
        this.state.update(clustering)
    }
    
    
    
    
}
