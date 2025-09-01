import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class MeteringTest {
    
    @Test
    fun testMeteringConsumerFunctionality() {
        println("Testing Metering Consumer without Kafka...")
        
        val dataSource = ConfigurationHelper.getDatabaseConfig()
        val kafkaConfig = ConfigurationHelper.getKafkaConfig()
        
        val consumer = MeteringConsumer(dataSource, kafkaConfig)
        
        assertDoesNotThrow {
            consumer.loadMetersCache()
        }
        
        val snappedTime = consumer.snapTimeToWindow("2025-08-04T10:15:30Z", "HOUR")
        assertNotNull(snappedTime)
        
        val testData = mapOf("total_requests" to 42)
        val value = consumer.extractValueFromEvent(testData, "$.data.total_requests")
        assertEquals(42.0, value)
        
        println("All tests passed!")
    }
}