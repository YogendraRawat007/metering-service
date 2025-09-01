import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

fun main() {
    println("Testing Metering Consumer without Kafka...")
    
    val dataSource = ConfigurationHelper.getDatabaseConfig()
    val kafkaConfig = ConfigurationHelper.getKafkaConfig()
    
    val consumer = MeteringConsumer(dataSource, kafkaConfig)
    
    println("Loading meters cache...")
    consumer.loadMetersCache()
    
    println("Testing with sample events...")
    val sampleEvents = SampleUsageEvents.createSampleEvents()
    val mapper = jacksonObjectMapper()
    
    sampleEvents.forEach { eventJson ->
        try {
            val usageEvent = mapper.readValue<UsageEvent>(eventJson)
            println("Processing event: ${usageEvent.id}")
            consumer.processUsageEvent(usageEvent)
            println("Successfully processed event: ${usageEvent.id}")
        } catch (e: Exception) {
            println("Error processing event: ${e.message}")
        }
    }
    
    println("Test completed!")
}