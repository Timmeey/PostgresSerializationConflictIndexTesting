package com.example

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.SQLException
import kotlin.test.assertEquals
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch
import java.sql.Connection

@Testcontainers
class DatabaseTest {

        @Container
        private val postgresContainer = PostgreSQLContainer<Nothing>("postgres:13")
        
        private lateinit var dbManager: DatabaseManager
        
        @BeforeEach
        fun setup() {
            postgresContainer.start()
            dbManager = DatabaseManager(
                postgresContainer.jdbcUrl,
                postgresContainer.username,
                postgresContainer.password
            )
            dbManager.connect()
        }
        
        @AfterEach
        fun teardown() {
            dbManager.disconnect()
            postgresContainer.stop()
        }

    @Test
    fun testInsertTransactionWithDetails() {
        println("Lets go")
        try {
            // Start a transaction
            dbManager.executeQuery("BEGIN")
        


            // Insert a new transaction and get the id
            val result = dbManager.executeQuery("""
                INSERT INTO transactions (customer_name)
                VALUES ('Hans 1')
                RETURNING id
            """.trimIndent())
            val transactionId = result[0]["id"] as Int


            // Insert transaction details
            val explainResult = dbManager.executeQuery("""
                EXPLAIN
                INSERT INTO transaction_details (transaction_id, product_name)
                VALUES 
                ($transactionId, 'Product A'),
                ($transactionId, 'Product B')
            """.trimIndent())
            
            // Print the explain analyze result
            println("Explain Analyze Result:")
            explainResult.forEach { println(it) }

            // Now execute the actual insert
            dbManager.executeQuery("""
                INSERT INTO transaction_details (transaction_id, product_name)
                VALUES 
                ($transactionId, 'Product A'),
                ($transactionId, 'Product B')
            """.trimIndent())

            // Commit the transaction
            dbManager.executeQuery("COMMIT")
        // Verify the inserted entries
        val transactionResult = dbManager.executeQuery("SELECT * FROM transactions WHERE customer_name = 'Hans 1'")
        assertEquals(1, transactionResult.size, "One transaction should be inserted")
        val insertedTransactionId = transactionResult[0]["id"] as Int

        val detailsResult = dbManager.executeQuery("SELECT * FROM transaction_details WHERE transaction_id = $insertedTransactionId")
        assertEquals(2, detailsResult.size, "Two transaction details should be inserted")
        
        val productNames = detailsResult.map { it["product_name"] as String }.sorted()
        assertEquals(listOf("Product A", "Product B"), productNames, "The inserted products should match")


        } catch (e: SQLException) {
            // If an error occurs, roll back the transaction
            println("Error occurred during transaction: ${e.message}")
            e.printStackTrace()
            dbManager.executeQuery("ROLLBACK")
            throw e
        }

        // Assert that no exception was thrown
        assertTrue(true, "No exception was thrown during the transaction")
    }

    //@Test
    fun testParallelInserts() {
        runParallelInsertTest(Connection.TRANSACTION_READ_COMMITTED, "Default Isolation")
    }

    @Test
    fun testParallelInsertsWithSerializable() {
        runParallelInsertTest(Connection.TRANSACTION_SERIALIZABLE, "SERIALIZABLE")
    }

    //@Test
    fun testParallelInsertsWithSerializableButWithHashIndex() {
        // Create a hash index on the transaction primary key
        dbManager.executeQuery("""
            CREATE INDEX IF NOT EXISTS transactions_id_hash_idx 
            ON transactions USING HASH (id)
        """.trimIndent())
        println("Created hash index on transactions(id)")
        runParallelInsertTest(Connection.TRANSACTION_SERIALIZABLE, "SERIALIZABLE")
    }

    private fun runParallelInsertTest(isolationLevel: Int, testName: String) {
        val recordsPerThread = 10_000
        val threadCount = 50
        val totalRecords = recordsPerThread * threadCount

        val exceptions = AtomicInteger(0)
        val latch = CountDownLatch(threadCount)

        val overallStartTime = System.nanoTime()

        val threads = (1..threadCount).map { threadId ->
            Thread {
                val threadConnection = dbManager.getNewConnection()
                threadConnection.transactionIsolation = isolationLevel
                val threadStartTime = System.nanoTime()
                var successfulInserts = 0

                for (i in 1..recordsPerThread) {
                    try {
                        insertRecord(threadConnection, threadId, i)
                        successfulInserts++
                    } catch (e: SQLException) {
                        exceptions.incrementAndGet()
                    }
                }

                threadConnection.close()

                val threadEndTime = System.nanoTime()
                val threadDuration = (threadEndTime - threadStartTime) / 1_000_000.0 // Convert to milliseconds

                println("Thread $threadId completed ($testName):")
                println("  Successful inserts: $successfulInserts")
                println("  Total time: $threadDuration ms")
                println("  Average time per record: ${threadDuration / recordsPerThread} ms")

                latch.countDown()
            }
        }

        threads.forEach { it.start() }
        latch.await()

        val overallEndTime = System.nanoTime()
        val overallDuration = (overallEndTime - overallStartTime) / 1_000_000.0 // Convert to milliseconds

        println("Overall results ($testName):")
        println("  Total records attempted: $totalRecords")
        println("  Total exceptions: ${exceptions.get()}")
        println("  Total time: $overallDuration ms")
        println("  Average time per record: ${overallDuration / totalRecords} ms")
    }

    private fun insertRecord(connection: Connection, threadId: Int, recordNumber: Int) {
        try {
            dbManager.executeQuery("BEGIN", connection)
            
            // Insert into transactions table
            val result = dbManager.executeQuery("""
                INSERT INTO transactions (customer_name)
                VALUES ('Customer ${threadId}_$recordNumber')
                RETURNING id
            """.trimIndent(), connection)
            val transactionId = result[0]["id"] as Int

            // Insert into transaction_details table
            dbManager.executeQuery("""
                INSERT INTO transaction_details (transaction_id, product_name)
                VALUES 
                ($transactionId, 'Product A'),
                ($transactionId, 'Product B')
            """.trimIndent(), connection)
            // Add 9ms delay
            Thread.sleep(9)

            dbManager.executeQuery("COMMIT", connection)
        } catch (e: SQLException) {
            dbManager.executeQuery("ROLLBACK", connection)
            throw e
        }
    }

    //@Test
    fun `test serialization conflict with serializable isolation level`() {
        // Insert initial data
        dbManager.executeQuery("INSERT INTO transactions (customer_name) VALUES ('Initial Customer')")
        
        val startLatch = CountDownLatch(1)
        val thread1ReadyLatch = CountDownLatch(1)
        val thread2ReadyLatch = CountDownLatch(1)
        val completionLatch = CountDownLatch(2)
        val exceptions = AtomicInteger(0)

        val threads = listOf(
            Thread {
                try {
                    val connection = dbManager.getNewConnection()
                    connection.use { conn ->
                        conn.transactionIsolation = Connection.TRANSACTION_SERIALIZABLE
                        conn.autoCommit = false

                        dbManager.executeQuery("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", conn)
                        dbManager.executeQuery("BEGIN", conn)

                        // Both threads read the initial data
                        val initialCount = dbManager.executeQuery("SELECT COUNT(*) as count FROM transactions", conn)[0]["count"] as Long

                        thread1ReadyLatch.countDown()
                        startLatch.await()

                        // First thread inserts a new record
                        dbManager.executeQuery("INSERT INTO transactions (customer_name) VALUES ('Customer A')", conn)
                        
                        thread2ReadyLatch.await()

                        // Both threads try to commit
                        conn.commit()
                    }
                } catch (e: Exception) {
                    exceptions.incrementAndGet()
                    println("Thread 1 exception: ${e.message}")
                } finally {
                    completionLatch.countDown()
                }
            },
            Thread {
                try {
                    val connection = dbManager.getNewConnection()
                    connection.use { conn ->
                        conn.transactionIsolation = Connection.TRANSACTION_SERIALIZABLE
                        conn.autoCommit = false

                        dbManager.executeQuery("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", conn)
                        dbManager.executeQuery("BEGIN", conn)

                        // Both threads read the initial data
                        val initialCount = dbManager.executeQuery("SELECT COUNT(*) as count FROM transactions", conn)[0]["count"] as Long

                        thread2ReadyLatch.countDown()
                        thread1ReadyLatch.await()
                        startLatch.await()

                        // Second thread also tries to insert based on the initial count
                        dbManager.executeQuery("INSERT INTO transactions (customer_name) VALUES ('Customer B')", conn)

                        // Both threads try to commit
                        conn.commit()
                    }
                } catch (e: Exception) {
                    exceptions.incrementAndGet()
                    println("Thread 2 exception: ${e.message}")
                } finally {
                    completionLatch.countDown()
                }
            }
        )

        threads.forEach { it.start() }
        
        // Wait for both threads to be ready, then start them simultaneously
        thread1ReadyLatch.await()
        thread2ReadyLatch.await()
        startLatch.countDown()

        completionLatch.await()

        assertEquals(1, exceptions.get(), "Expected one serialization conflict exception")

        // Verify the final state
        val result = dbManager.executeQuery("SELECT COUNT(*) as count FROM transactions")
        assertEquals(2, result[0]["count"] as Long, "Expected two transactions in total")
    }
}