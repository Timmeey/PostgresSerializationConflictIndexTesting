package com.example

fun main() {
    val dbManager = DatabaseManager()
    dbManager.connect()
    
    // Example query
    val users = dbManager.executeQuery("SELECT * FROM users")
    users.forEach { println(it) }
    
    dbManager.disconnect()
}