package ru.catcab.tool.database.synchronizer.models

class SyncStats(
    val start: Long,
    val table: String,
    val metrics: String
) {
    class Builder {
        var start: Long = 0
        var table: String = ""
        var metrics: String = ""

        fun build(): SyncStats {
            return SyncStats(start, table, metrics)
        }
    }

    fun builder() = Builder().also {
        it.start = start
        it.table = table
        it.metrics = metrics
    }
}