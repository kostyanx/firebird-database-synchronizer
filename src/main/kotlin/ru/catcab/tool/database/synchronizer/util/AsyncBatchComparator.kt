package ru.catcab.tool.database.synchronizer.util

import org.slf4j.LoggerFactory
import org.slf4j.MDC
import ru.catcab.tool.database.synchronizer.models.Column
import ru.catcab.tool.database.synchronizer.models.TableMeta
import ru.catcab.tool.database.synchronizer.service.NotifierService
import ru.catcab.utils.SynHolder
import ru.kostyanx.database.LocResultSet
import ru.kostyanx.dbproxy.ConnectionProxy
import java.io.Closeable
import java.sql.ResultSet
import java.util.concurrent.TimeUnit.SECONDS

class AsyncBatchComparator(
    private val tableMeta: TableMeta,
    private val rs: ResultSet,
    val dstConn: ConnectionProxy,
    private val notifierService: NotifierService
): Closeable {
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

    private var hasNext = rs.next()
    private var changes = 0
    private var total = 0

    companion object {
        private val LOG = LoggerFactory.getLogger(AsyncBatchComparator::class.java);
    }

    private fun compareByColumns(rs: ResultSet, lrs: LocResultSet<out LocResultSet<*>>, columns: List<Column>): Int {
        columns.forEach { column ->
            val rsVal = rs.getObject(column.ordinal)
            val lrsVal = lrs.getObject(column.ordinal - 1)
            val result = compareValues(rsVal as Comparable<*>?, lrsVal as Comparable<*>?)
            if (result != 0) {
                return result;
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
        MDC.put("table", tableMeta.table.name)
        LOG.info("compare() lrs.size: {}", lrs.size())
        var counter = 0
        while(hasNext && lrs.next()) {
            MDC.put("counter", counter++.toString())
            val result = compareByColumns(rs, lrs, keyColumns)
            when {
                result < 0 -> {
                    performDelete(keyColumns)
                    rs.next()
                }
                result > 0 -> performInsert(lrs)
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
                    hasNext = rs.next()
                }
            }
        }
        if (!hasNext) {
            while (lrs.next()) {
                performInsert(lrs)
            }
        }
        total += lrs.size()
        fireProgressEvent()
        MDC.remove("counter")
    }

    private fun performDelete(keyColumns: List<Column>) {
        LOG.info("delete")
        dstConn.executeUpdate(deleteSql, keyColumns.map { rs.getObject(it.ordinal) })
        changes++
    }

    private fun performInsert(lrs: LocResultSet<out LocResultSet<*>>) {
        LOG.info("insert")
        dstConn.executeInsert2(insertSql, columns.map { lrs.getObject(it.ordinal - 1) })
        changes++
    }

    private fun performUpdate(changedColumns: List<Triple<Any, Any, Column>>) {
        LOG.info("update")
        val updateColumns = changedColumns.map { it.third.name }
        val updateSql = "UPDATE $tableName SET ${updateColumns.joinToString(",") { "$it = ?" }}" +
                " WHERE ${pk.columns.joinToString(" AND ") { "$it = ?" }}"
        val params = ArrayList<Any?>()
        params.addAll(changedColumns.map { it.second })
        params.addAll(keyColumns.map { rs.getObject(it.ordinal) })
        dstConn.executeUpdate(updateSql, params)
        changes++
    }

    private fun onClose() {
        if (!rs.isClosed) {
            while (hasNext) {
                performDelete(keyColumns)
                hasNext = rs.next()
            }
        }
        fireProgressEvent()
        MDC.remove("table")
    }

    private fun fireProgressEvent() {
        notifierService.fire("syncProgress", Triple(tableMeta.table.name, changes, total))
    }

    fun submit(holder: SynHolder<out LocResultSet<out LocResultSet<*>>>) {
        asyncBatchExecutor.submit(holder)
    }

    override fun close() = asyncBatchExecutor.close()
}
