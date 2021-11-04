package com.example.server

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.HttpHost
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.BodyExtractors.toMono
import org.springframework.web.reactive.function.server.RequestPredicates.POST
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions.route
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import software.amazon.awssdk.enhanced.dynamodb.*
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
        dynamoDbClient: DynamoDbClient,
        objectMapper: ObjectMapper,
    ): RouterFunction<ServerResponse> {
        log.info("Initiating DynamoDB Async client")
        val client = DynamoDbEnhancedAsyncClient
            .builder()
            .dynamoDbClient(dynamoDbAsyncClient)
            .build()
        val table = client.table("assets", TableSchema.fromBean(Asset::class.java))
        val tableSync = tableSync(dynamoDbClient)
        log.info("Finished initiating DynamoDB Async client")

        log.info("Initiating Elasticsearch client")
        val es = RestHighLevelClient(RestClient.builder(HttpHost("localhost", 9200)))
        log.info("Finished initiating Elasticsearch client")
        
        return route(
            POST("/api/assets")
        ) { req ->
            req.body(toMono(SaveAssetsRequest::class.java))
                .flatMap { saveAssetsRequest ->
                    saveAssets(saveAssetsRequest, table, client, tableSync)
                    // now process Elasticsearch index asynchronously too
                    indexAssets(saveAssetsRequest, objectMapper, es)
                    ok().build()
                }
        }

    }

    private fun saveAssets(
        saveAssetsRequest: SaveAssetsRequest,
        table: DynamoDbAsyncTable<Asset>?,
        client: DynamoDbEnhancedAsyncClient,
        tableSync: DynamoDbTable<Asset>?
    ) {
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
    }

    private fun indexAssets(
        saveAssetsRequest: SaveAssetsRequest,
        objectMapper: ObjectMapper,
        es: RestHighLevelClient
    ) {
        val request = BulkRequest()
        saveAssetsRequest.assets.forEach { asset ->
            request.add(
                IndexRequest("assets")
                    .id(asset.assetId)
                    .source(objectMapper.writeValueAsString(asset), XContentType.JSON)
            )
        }
        es.bulkAsync(request, RequestOptions.DEFAULT, object : ActionListener<BulkResponse> {
            override fun onResponse(response: BulkResponse) {
                if (response.hasFailures()) {
                    val failureMessages =
                        response.mapNotNull { bulkItemResponse -> bulkItemResponse.failureMessage }
                    log.error(failureMessages.joinToString(";"))
                } else {
                    log.info("Success index complete")
                }
            }

            override fun onFailure(e: Exception) {
                log.error("Request error while indexing assets", e)
            }
        })
    }

    private fun tableSync(dynamoDbClient: DynamoDbClient) = DynamoDbEnhancedClient
        .builder()
        .dynamoDbClient(dynamoDbClient)
        .build()
        .table("assets", TableSchema.fromBean(Asset::class.java))
}