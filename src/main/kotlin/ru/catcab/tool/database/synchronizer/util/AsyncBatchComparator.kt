package ru.catcab.tool.database.synchronizer.util

import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import ru.catcab.tool.database.synchronizer.models.Column
import ru.catcab.tool.database.synchronizer.models.SyncOptions
import ru.catcab.tool.database.synchronizer.models.SyncStats
import ru.catcab.tool.database.synchronizer.models.TableMeta
import ru.catcab.utils.SynHolder
import ru.kostyanx.database.LocResultSet
import ru.kostyanx.dbproxy.ConnectionProxy
import java.io.Closeable
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types.*
import java.util.concurrent.TimeUnit.SECONDS

class AsyncBatchComparator(
    tableMeta: TableMeta,
    private val rs: ResultSet,
    val dstConn: ConnectionProxy,
    private val syncOptions: SyncOptions,
    private var syncStats: SyncStats
) : Closeable {
    companion object {
        private val LOG = LoggerFactory.getLogger(AsyncBatchComparator::class.java)
        private val intTypes = listOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }

    private val pk = tableMeta.primaryKey ?: throw IllegalArgumentException("primary key is null")
    private val tableName = tableMeta.table.name
    private val columns = tableMeta.columns.filter { pk.columns.contains(it.name) || !it.isGeneratedColumn }
    private val cols = columns.map { it.name }
    private val columnMap = columns.associateBy { it.name }
    private val keyColumns = pk.columns.map { columnMap[it] ?: error("invalid column") }
    private val noKeyColumns = if (keyColumns.size < columns.size) columns.filter { !keyColumns.contains(it) } else emptyList()

    private val deleteSql = "DELETE FROM $tableName WHERE ${pk.columns.joinToString(" AND ") { "$it = ?" }}"
    private val insertSql = "INSERT INTO $tableName (${cols.joinToString(",")}) VALUES (${cols.joinToString(",") {"?"}})"

    private val asyncBatchExecutor = AsyncBatchExecutor(LOG, this::compare, this::onClose, 60, SECONDS)


    private val deleteConditionBuilder = pk.columns.singleOrNull()
        ?.takeIf { columnMap[it]?.dataType in intTypes }
        ?.let { key -> DeleteConditionBuilder<List<Any>>(key, 10, 500) { (it[0] as Number).toLong() } }

    private val deleteBatch: MutableList<List<Any>> = mutableListOf()
    private val insertBatch: MutableList<List<Any>> = mutableListOf()
    private val updateBatch = ArrayList<Pair<String, List<Any?>>>()

    private var rsHasValue = rs.next()
    private var changes = 0
    private var counter = 0

    private fun compareByColumns(rs: ResultSet, lrs: LocResultSet<out LocResultSet<*>>, columns: List<Column>): Int {
        columns.forEach { column ->
            val rsVal = rs.getObject(column.ordinal)
            val lrsVal = lrs.getObject(column.ordinal - 1)
            val result = compareValues(rsVal as Comparable<*>?, lrsVal as Comparable<*>?)
            if (result != 0) {
                return result
            }
        }
        return 0
    }

    private fun equalsByColumns(rs: ResultSet, lrs: LocResultSet<out LocResultSet<*>>, columns: List<Column>): Boolean {
        for (column in columns) {
            val rsVal = rs.getObject(column.ordinal)
            val lrsVal = lrs.getObject(column.ordinal - 1)

            if (rsVal is ByteArray && lrsVal is ByteArray && rsVal contentEquals lrsVal) {
                continue
            }

            if (rsVal != lrsVal) {
                return false
            }
        }
        return true
    }

    private fun compare(lrs: LocResultSet<out LocResultSet<*>>) {
        MDC.put("table", tableName)
        var lrsHasValue = lrs.next()
        while (rsHasValue && lrsHasValue) {
            MDC.put("counter", counter++.toString())
            val result = compareByColumns(rs, lrs, keyColumns)
            when {
                result < 0 -> {
                    performDelete()
                    rsHasValue = rs.next()
                }
                result > 0 -> {
                    performInsert(lrs)
                    lrsHasValue = lrs.next()
                }
                result == 0 -> {
                    if (!equalsByColumns(rs, lrs, noKeyColumns)) {
                        val changedColumns = noKeyColumns.map { column ->
                            val rsVal = rs.getObject(column.ordinal)
                            val lrsVal = lrs.getObject(column.ordinal - 1)
                            Triple(rsVal, lrsVal, column)
                        }.filter { it.first != it.second }
                        if (changedColumns.isNotEmpty()) {
                            performUpdate(changedColumns)
                        }
                    }
                    lrsHasValue = lrs.next()
                    rsHasValue = rs.next()
                }
            }
            if (counter % 1000 == 0) fireProgressEvent()
        }
        while (lrsHasValue) {
            MDC.put("counter", counter++.toString())
            performInsert(lrs)
            lrsHasValue = lrs.next()
            if (counter % 1000 == 0) fireProgressEvent()
        }
        fireProgressEvent()
    }

    private fun performDelete() {
        deleteBatch += keyColumns.map { rs.getObject(it.ordinal) }
        performDeleteBatch(false)
        changes++
    }

    private fun performDeleteBatch(force: Boolean) {
        if (deleteConditionBuilder != null) {
            optimizedExecuteDeleteBatch(force, deleteConditionBuilder)
        } else {
            executeBatch(force, deleteSql, deleteBatch)
        }
    }

    private fun optimizedExecuteDeleteBatch(force: Boolean, deleteConditionBuilder: DeleteConditionBuilder<List<Any>>) {
        if (!force && deleteBatch.size < 500) return
        val requireUpdate = deleteConditionBuilder.addAndCheck(deleteBatch)
        deleteBatch.clear()
        if (requireUpdate || force && deleteConditionBuilder.totalSize > 0) {
            val conditions = deleteConditionBuilder.generateConditions()
            val sql = "delete from $tableName where ${conditions.conditions} ROWS ?"
            val args = conditions.args.plus(deleteConditionBuilder.totalSize)
            dstConn.executeUpdate(sql, args)
            deleteConditionBuilder.reset()
        }
    }

    private fun performInsert(lrs: LocResultSet<out LocResultSet<*>>) {
        insertBatch += columns.map { lrs.getObject(it.ordinal - 1) }
        performInsertBatch(false)
        changes++
    }

    private fun performInsertBatch(force: Boolean) {
        executeBatch(force, insertSql, insertBatch)
    }

    private fun executeBatch(force: Boolean, sql: String, batch: MutableList<List<Any>>) {
        if (!force && batch.size < 1000) return
        if (force && batch.isEmpty()) return
        dstConn.execute { conn: Connection -> conn.executeBatch(sql, batch) }
        batch.clear()
    }

    private fun performUpdate(changedColumns: List<Triple<Any, Any, Column>>) {
        val updateColumns = changedColumns.map { it.third.name }
        @Language("GenericSQL")
        val updateSql = "UPDATE $tableName SET ${updateColumns.joinToString(",") { "$it = ?" }}" +
                " WHERE ${pk.columns.joinToString(" AND ") { "$it = ?" }}"
        val params = ArrayList<Any?>()
        params.addAll(changedColumns.map { it.second })
        params.addAll(keyColumns.map { rs.getObject(it.ordinal) })
        updateBatch += updateSql to params
        performUpdateBatch(false)
        changes++
    }

    private fun performUpdateBatch(force: Boolean) {
        if (!force && updateBatch.size < 1000) return
        if (force && updateBatch.isEmpty()) return
        updateBatch.groupBy { it.first }.forEach { (sql, rows) ->
            dstConn.execute { conn: Connection ->
                conn.prepareStatement(sql).use { statement ->
                    for (row in rows) {
                        statement.setParameters(row.second)
                        statement.addBatch()
                    }
                    statement.executeBatch()
                }
            }
        }
        updateBatch.clear()
    }

    private fun onClose() {
        if (!rs.isClosed) {
            while (rsHasValue) {
                MDC.put("counter", counter++.toString())
                performDelete()
                if (counter % 1000 == 0) fireProgressEvent()
                rsHasValue = rs.next()
            }
        }
        performDeleteBatch(true)
        performInsertBatch(true)
        performUpdateBatch(true)
        fireProgressEvent()
        MDC.remove("counter")
        MDC.remove("table")
    }

    private fun fireProgressEvent() {
        syncStats = syncStats.builder().apply { metrics = "$changes / ${counter}" }.build()
        syncOptions.statListener(syncStats)
    }

    fun submit(holder: SynHolder<out LocResultSet<out LocResultSet<*>>>) {
        asyncBatchExecutor.submit(holder)
    }

    override fun close() = asyncBatchExecutor.close()

    private fun Int.toComparingSign(): Char {
        return when {
            this < 0 -> '<'
            this == 0 -> '='
            this > 0 -> '>'
            else -> throw IllegalStateException("is not a number?")
        }
    }

    private fun PreparedStatement.setParameters(row: List<Any?>) {
        var i = 0
        @Suppress("UseWithIndex")
        for (data in row) {
            setObject(++i, data)
        }
    }

    private fun Connection.executeBatch(sql: String, data: MutableList<List<Any>>) {
        prepareStatement(sql).use { statement ->
            for (row in data) {
                statement.setParameters(row)
                statement.addBatch()
            }
            statement.executeBatch()
        }
    }

}


