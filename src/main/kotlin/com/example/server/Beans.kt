package com.example.server

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

@Configuration
class Beans {
    @Bean
    fun dynamoDbAsyncClient(): DynamoDbAsyncClient {
        return DynamoDbAsyncClient.builder().build()
    }
    
    @Bean
    fun dynamoDbClient(): DynamoDbClient {
        return DynamoDbClient.builder().build()
    }
}