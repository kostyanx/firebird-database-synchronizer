package ru.catcab.tool.database.synchronizer.models

enum class Direction(val jdbcVal: String) {
    ASC("A"),
    DESC("D");

    companion object {
        fun String.toDirection(): Direction {
            return values().single { it.jdbcVal == this }
        }
    }
}