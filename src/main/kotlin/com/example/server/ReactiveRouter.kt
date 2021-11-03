package com.example.server

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.BodyExtractors.toMono
import org.springframework.web.reactive.function.server.RequestPredicates.POST
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions.route
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.TableSchema
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

data class SaveAssetsRequest(
    val assets: List<Asset>
)

@Configuration
class ReactiveRouter {

    private val log = LoggerFactory.getLogger(ReactiveRouter::class.java)

    @Bean
    fun postAccount(
        dynamoDbAsyncClient: DynamoDbAsyncClient,
        dynamoDbClient: DynamoDbClient
    ): RouterFunction<ServerResponse> {

        log.info("Initiating DynamoDB Async client")
        val client = DynamoDbEnhancedAsyncClient
            .builder()
            .dynamoDbClient(dynamoDbAsyncClient)
            .build()
        val table = client.table("assets", TableSchema.fromBean(Asset::class.java))
        val tableSync = tableSync(dynamoDbClient)
        log.info("Finished initiating DynamoDB Async client")

        return route(
            POST("/api/assets")
        ) { req ->
            req.body(toMono(SaveAssetsRequest::class.java))
                .flatMap { saveAssetsRequest ->
                    val batches = saveAssetsRequest.assets.map { asset ->
                        WriteBatch
                            .builder(Asset::class.java)
                            .addPutItem(asset)
                            .mappedTableResource(table)
                            .build()
                    }
                    client.batchWriteItem(
                        BatchWriteItemEnhancedRequest
                            .builder()
                            .writeBatches(batches)
                            .build()
                    ).handle { result, exception ->
                        if (exception != null) {
                            log.error("An error occurred", exception)
                        } else {
                            val unprocessedPutItemsForTable = result.unprocessedPutItemsForTable(tableSync)
                            log.info("Success - unprocessed=${unprocessedPutItemsForTable.size}")
                        }
                    }
                    ok().build()
                }
        }

    }

    private fun tableSync(dynamoDbClient: DynamoDbClient) = DynamoDbEnhancedClient
        .builder()
        .dynamoDbClient(dynamoDbClient)
        .build()
        .table("assets", TableSchema.fromBean(Asset::class.java))
}