import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.jayway.jsonpath.JsonPath
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.postgresql.ds.PGSimpleDataSource
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

@JsonIgnoreProperties(ignoreUnknown = true)
data class MeterConfig(
    val meterId: Int,
    val externalId: String,
    val cdfService: String,
    val metricKey: String,
    val eventType: String,
    val valueProperty: String,
    val aggregation: String,
    val groupBy: Map<String, Any> = emptyMap(),
    val windowSize: String
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class UsageEvent(
    val type: String,
    val time: String,
    val id: String,
    val source: String,
    val project: String,
    val data: Map<String, Any>,
    val metadata: Map<String, Any>? = null
)

class MeteringConsumer(
    private val dataSource: DataSource,
    private val kafkaConfig: Properties
) {
    private val logger = LoggerFactory.getLogger(MeteringConsumer::class.java)
    private val objectMapper: ObjectMapper = jacksonObjectMapper()
    private val metersCache = ConcurrentHashMap<String, List<MeterConfig>>()
    
    fun loadMetersCache() {
        dataSource.connection.use { conn ->
            val query = """
                SELECT meter_id, external_id, cdf_service, metric_key, event_type,
                       value_property, aggregation, group_by, window_size
                FROM meters
            """
            
            conn.prepareStatement(query).use { stmt ->
                val rs = stmt.executeQuery()
                val metersMap = mutableMapOf<String, MutableList<MeterConfig>>()
                
                while (rs.next()) {
                    val meter = MeterConfig(
                        meterId = rs.getInt("meter_id"),
                        externalId = rs.getString("external_id"),
                        cdfService = rs.getString("cdf_service"),
                        metricKey = rs.getString("metric_key"),
                        eventType = rs.getString("event_type"),
                        valueProperty = rs.getString("value_property"),
                        aggregation = rs.getString("aggregation"),
                        groupBy = parseJsonToMap(rs.getString("group_by")),
                        windowSize = rs.getString("window_size")
                    )
                    
                    val key = "${meter.cdfService}:${meter.eventType}"
                    metersMap.computeIfAbsent(key) { mutableListOf() }.add(meter)
                }
                
                metersCache.putAll(metersMap)
                logger.info("Loaded ${metersCache.size} meter configurations")
            }
        }
    }
    
    fun findAssociatedMeters(source: String, eventType: String): List<MeterConfig> {
        val key = "$source:$eventType"
        return metersCache[key] ?: emptyList()
    }
    
    fun snapTimeToWindow(timestamp: String, windowSize: String): LocalDateTime {
        val zonedDateTime = ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_ZONED_DATE_TIME)
        val localDateTime = zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime()
        
        return when (windowSize.uppercase()) {
            "MINUTE" -> localDateTime.truncatedTo(ChronoUnit.MINUTES)
            "HOUR" -> localDateTime.truncatedTo(ChronoUnit.HOURS)
            "DAY" -> localDateTime.truncatedTo(ChronoUnit.DAYS)
            "MONTH" -> localDateTime.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)
            else -> {
                logger.warn("Unknown window_size: $windowSize, defaulting to DAY")
                localDateTime.truncatedTo(ChronoUnit.DAYS)
            }
        }
    }
    
    fun extractValueFromEvent(eventData: Map<String, Any>, valueProperty: String): Double {
        return try {
            val fullData = mapOf("data" to eventData)
            val result = JsonPath.read<Any>(fullData, valueProperty)
            
            when (result) {
                is Number -> result.toDouble()
                is String -> result.toDoubleOrNull() ?: 0.0
                else -> {
                    logger.error("Invalid value type for JSONPath: $valueProperty")
                    0.0
                }
            }
        } catch (e: Exception) {
            logger.error("Error extracting value with JSONPath $valueProperty", e)
            0.0
        }
    }
    
    fun extractGroupByDimensions(eventData: Map<String, Any>, groupBy: Map<String, Any>): Map<String, Any> {
        val dimensions = mutableMapOf<String, Any>()
        val fullData = mapOf("data" to eventData)
        
        groupBy.forEach { (dimName, jsonPathExpr) ->
            try {
                val result = JsonPath.read<Any>(fullData, jsonPathExpr.toString())
                dimensions[dimName] = result
            } catch (e: Exception) {
                logger.error("Error extracting group-by dimension $dimName", e)
            }
        }
        
        return dimensions
    }
    
    fun insertAggregatedUsage(
        conn: Connection,
        meter: MeterConfig,
        usageEvent: UsageEvent,
        snappedTime: LocalDateTime,
        value: Double,
        groupByDims: Map<String, Any>
    ) {
        val query = """
            INSERT INTO aggregated_usage 
            (time, project_id, cdf_service, metric_key, group_by_dims, value_sum, value_count)
            VALUES (?, ?, ?, ?, ?::jsonb, ?, ?)
            ON CONFLICT (time, project_id, cdf_service, metric_key, (group_by_dims::text))
            DO UPDATE SET
                value_sum = aggregated_usage.value_sum + EXCLUDED.value_sum,
                value_count = aggregated_usage.value_count + EXCLUDED.value_count
        """
        
        try {
            conn.prepareStatement(query).use { stmt ->
                stmt.setTimestamp(1, Timestamp.valueOf(snappedTime))
                stmt.setString(2, usageEvent.project)
                stmt.setString(3, meter.cdfService)
                stmt.setString(4, meter.metricKey)
                stmt.setString(5, objectMapper.writeValueAsString(groupByDims))
                stmt.setDouble(6, value)
                stmt.setInt(7, 1)
                
                stmt.executeUpdate()
                conn.commit()
                
                logger.info("Aggregated usage inserted/updated for meter ${meter.externalId}")
            }
        } catch (e: Exception) {
            logger.error("Error inserting aggregated usage", e)
            conn.rollback()
            throw e
        }
    }
    
    fun processUsageEvent(usageEvent: UsageEvent) {
        logger.info("Processing usage event: ${usageEvent.id}")
        
        val source = usageEvent.source
        val eventType = usageEvent.type
        
        val associatedMeters = findAssociatedMeters(source, eventType)
        
        if (associatedMeters.isEmpty()) {
            logger.warn("No meters found for source=$source, event_type=$eventType")
            return
        }
        
        logger.info("Found ${associatedMeters.size} associated meters")
        
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            
            associatedMeters.forEach { meter ->
                try {
                    val snappedTime = snapTimeToWindow(usageEvent.time, meter.windowSize)
                    val value = extractValueFromEvent(usageEvent.data, meter.valueProperty)
                    val groupByDims = extractGroupByDimensions(usageEvent.data, meter.groupBy)
                    insertAggregatedUsage(conn, meter, usageEvent, snappedTime, value, groupByDims)
                } catch (e: Exception) {
                    logger.error("Error processing meter ${meter.externalId}", e)
                }
            }
        }
    }
    
    fun consumeFromKafka(topic: String = "usage-events") {
        val consumer = KafkaConsumer<String, String>(kafkaConfig).apply {
            subscribe(listOf(topic))
        }
        
        logger.info("Starting Kafka consumer for topic: $topic")
        
        try {
            while (true) {
                val records = consumer.poll(java.time.Duration.ofMillis(1000))
                
                for (record in records) {
                    try {
                        val usageEvent = objectMapper.readValue<UsageEvent>(record.value())
                        processUsageEvent(usageEvent)
                        consumer.commitSync()
                        logger.info("Successfully processed and committed message: ${usageEvent.id}")
                    } catch (e: Exception) {
                        logger.error("Error processing message: ${record.value()}", e)
                    }
                }
            }
        } catch (e: InterruptedException) {
            logger.info("Consumer interrupted")
        } finally {
            consumer.close()
        }
    }
    
    private fun parseJsonToMap(jsonString: String?): Map<String, Any> {
        return if (jsonString.isNullOrBlank() || jsonString == "{}") {
            emptyMap()
        } else {
            try {
                objectMapper.readValue(jsonString)
            } catch (e: Exception) {
                logger.error("Error parsing JSON: $jsonString", e)
                emptyMap()
            }
        }
    }
}

class MeteringConsumerService(
    private val dataSource: DataSource,
    private val kafkaConfig: Properties
) {
    private val logger = LoggerFactory.getLogger(MeteringConsumerService::class.java)
    private val consumer = MeteringConsumer(dataSource, kafkaConfig)
    private var job: Job? = null
    
    fun start(topic: String = "usage-events") = runBlocking {
        logger.info("Starting Metering Consumer Service")
        consumer.loadMetersCache()
        job = launch {
            consumer.consumeFromKafka(topic)
        }
        job?.join()
    }
    
    fun stop() {
        logger.info("Stopping Metering Consumer Service")
        job?.cancel()
    }
}

object ConfigurationHelper {
    fun getDatabaseConfig(): DataSource {
        val dataSource = PGSimpleDataSource()
        dataSource.serverNames = arrayOf(System.getenv("DB_HOST") ?: "localhost")
        dataSource.databaseName = System.getenv("DB_NAME") ?: "metering_db"
        dataSource.user = System.getenv("DB_USER") ?: "admin"
        dataSource.password = System.getenv("DB_PASSWORD") ?: "password"
        dataSource.portNumbers = intArrayOf(System.getenv("DB_PORT")?.toInt() ?: 5432)
        return dataSource
    }
    
    fun getKafkaConfig(): Properties {
        return Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                System.getenv("KAFKA_SERVERS") ?: "localhost:9092")
            put(ConsumerConfig.GROUP_ID_CONFIG, 
                System.getenv("KAFKA_GROUP_ID") ?: "metering-consumer-group")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }
    }
}

object SampleUsageEvents {
    fun createSampleEvents(): List<String> {
        val events = listOf(
            UsageEvent(
                type = "request",
                time = "2025-08-04T10:15:00Z",
                id = "2d1b7e41-0164-42b4-82a1-00236a2826a7",
                source = "Timeseries",
                project = "AkerBP",
                data = mapOf("total_requests" to 10)
            ),
            UsageEvent(
                type = "atlas_event",
                time = "2025-07-11T10:15:00Z",
                id = "e22d99d3-3f1a-4122-b91c-8b83984d720f",
                source = "Atlas",
                project = "cognite2dev",
                data = mapOf(
                    "prompts_used" to 5,
                    "coins_consumed" to 10
                )
            ),
            UsageEvent(
                type = "request",
                time = "2025-07-11T10:15:00Z",
                id = "a33d99d3-3f1a-4122-b91c-8b83984d720f",
                source = "Data Modelling",
                project = "cognite2dev",
                data = mapOf("total_requests" to 5)
            )
        )
        
        val mapper = jacksonObjectMapper()
        return events.map { mapper.writeValueAsString(it) }
    }
}

class MeteringConsumerTest {
    private val logger = LoggerFactory.getLogger(MeteringConsumerTest::class.java)
    
    fun testMeteringConsumer() {
        val dataSource = ConfigurationHelper.getDatabaseConfig()
        val kafkaConfig = ConfigurationHelper.getKafkaConfig()
        
        val consumer = MeteringConsumer(dataSource, kafkaConfig)
        consumer.loadMetersCache()
        val sampleEvents = SampleUsageEvents.createSampleEvents()
        val mapper = jacksonObjectMapper()
        
        sampleEvents.forEach { eventJson ->
            try {
                val usageEvent = mapper.readValue<UsageEvent>(eventJson)
                consumer.processUsageEvent(usageEvent)
                logger.info("Successfully tested event: ${usageEvent.id}")
            } catch (e: Exception) {
                logger.error("Error testing event: $eventJson", e)
            }
        }
    }
}

fun main() {
    val dataSource = ConfigurationHelper.getDatabaseConfig()
    val kafkaConfig = ConfigurationHelper.getKafkaConfig()
    
    val service = MeteringConsumerService(dataSource, kafkaConfig)
    
    Runtime.getRuntime().addShutdownHook(Thread {
        service.stop()
    })
    
    try {
        service.start()
    } catch (e: Exception) {
        println("Error starting service: ${e.message}")
        service.stop()
    }
}

fun ResultSet.getJsonMap(columnName: String): Map<String, Any> {
    val jsonString = this.getString(columnName)
    return if (jsonString.isNullOrBlank() || jsonString == "{}") {
        emptyMap()
    } else {
        try {
            jacksonObjectMapper().readValue(jsonString)
        } catch (e: Exception) {
            emptyMap()
        }
    }
}