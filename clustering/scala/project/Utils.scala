package project

import net.liftweb.json.DefaultFormats
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import net.liftweb.json.Serialization.write

class OutKafkaJsonSchema extends SerializationSchema[List[List[Measurement]]] {
    
    override def serialize(t: List[List[Measurement]]): Array[Byte] = {
        implicit val formats = DefaultFormats
        write(t).getBytes()
    }
    
}

class InKafkaJsonSchema extends SerializationSchema[ObjectNode] with DeserializationSchema[ObjectNode] {
    
    import org.apache.flink.api.common.typeinfo.TypeInformation
    import org.apache.flink.api.java.typeutils.TypeExtractor
    import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
    
    override def serialize(t: ObjectNode): Array[Byte] = t.toString().getBytes("UTF-8")
    
    override def isEndOfStream(t: ObjectNode): Boolean = false
    
    override def deserialize(bytes: Array[Byte]): ObjectNode = {
        try {
            new ObjectMapper().readValue(new String(bytes, "UTF-8"), classOf[ObjectNode])
        } catch {
            case e: Exception => new ObjectMapper().readValue("{}", classOf[ObjectNode])
        }
    }
    
    override def getProducedType: TypeInformation[ObjectNode] = TypeExtractor.getForClass(classOf[ObjectNode])
}

class ListAggregateFunction[T] extends AggregateFunction[T, List[T], List[T]] {
    
    override def createAccumulator(): List[T] = List[T]()
    
    override def add(value: T, acc: List[T]): List[T] = value :: acc
    
    override def getResult(acc: List[T]): List[T] = acc
    
    override def merge(a: List[T], b: List[T]): List[T] = a ::: b
    
}


class Measurement(var timestamp: Long = 0, var location_id: Long = 0, var measurement: Double = 0.0) {
    
    var _timestamp: Long = timestamp;
    var _location_id: Long = location_id;
    var _measurement: Double = measurement;
    
}

