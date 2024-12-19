package com.example

import java.sql.Connection
import java.sql.DriverManager
import org.flywaydb.core.Flyway

class DatabaseManager(
    private val jdbcUrl: String = "jdbc:postgresql://localhost:5432/testdb",
    private val username: String = "testuser",
    private val password: String = "testpassword"
) {
    private var mainConnection: Connection? = null
    
    fun connect() {
        mainConnection = DriverManager.getConnection(jdbcUrl, username, password)
        
        // Run Flyway migrations
        val flyway = Flyway.configure()
            .dataSource(jdbcUrl, username, password)
            .locations("classpath:db/migration")
            .load()
        flyway.migrate()
    }
    
    fun getNewConnection(): Connection {
        return DriverManager.getConnection(jdbcUrl, username, password)
    }
    
    fun executeQuery(sql: String, connection: Connection? = null): List<Map<String, Any>> {
        val conn = connection ?: mainConnection
        val statement = conn?.createStatement()
        val isResultSet = statement?.execute(sql) ?: false
        val results = mutableListOf<Map<String, Any>>()
        
        if (isResultSet) {
            val resultSet = statement?.resultSet
            while (resultSet?.next() == true) {
                val row = mutableMapOf<String, Any>()
                for (i in 1..resultSet.metaData.columnCount) {
                    val columnName = resultSet.metaData.getColumnName(i)
                    val value = resultSet.getObject(i)
                    row[columnName] = value
                }
                results.add(row)
            }
            resultSet?.close()
        } else {
            val updateCount = statement?.updateCount ?: 0
            results.add(mapOf("affected_rows" to updateCount))
        }
        
        statement?.close()
        return results
    }
    
    fun disconnect() {
        mainConnection?.close()
    }
}