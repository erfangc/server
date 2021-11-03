package com.example.server

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey
import java.time.Instant

@DynamoDbBean
data class Asset(
    @get:DynamoDbPartitionKey
    var assetId: String? = null,
    var assetType: String? = null,
    var updated: Instant = Instant.now(),
    var entitlements: Map<String, List<String>> = emptyMap()
)