package ru.catcab.tool.database.synchronizer.util

import org.slf4j.LoggerFactory
import org.slf4j.MDC
import ru.catcab.tool.database.synchronizer.models.Column
import ru.catcab.tool.database.synchronizer.models.TableMeta
import ru.catcab.tool.database.synchronizer.service.NotifierService
import ru.catcab.utils.SynHolder
import ru.kostyanx.database.LocResultSet
import java.io.Closeable
import java.sql.ResultSet
import java.util.*
import java.util.concurrent.TimeUnit.SECONDS

class AsyncBatchComparator(private val rs: ResultSet, private val tableMeta: TableMeta, private val notifierService: NotifierService): Closeable {
    private val asyncBatchExecutor = AsyncBatchExecutor(LOG, this::compare, this::onClose, 60, SECONDS)
    private var hasNext = false
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
        columns.forEach{ column ->
            val rsVal = rs.getObject(column.ordinal)
            val lrsVal = lrs.getObject(column.ordinal - 1)
            if (rsVal is ByteArray && lrsVal is ByteArray) {
                if (!rsVal.contentEquals(lrsVal)) {
                    return false
                }
            } else if (!Objects.equals(rsVal, lrsVal)) {
                return false
            }
        }
        return true
    }

    private fun compare(lrs: LocResultSet<out LocResultSet<*>>) {
        MDC.put("table", tableMeta.table.name)
        LOG.info("compare() lrs.size: {}", lrs.size())
        val columns = tableMeta.columns
        val columnMap = columns.associateBy { it.name }
        val keyColumns = tableMeta.primaryKey!!.columns.map { columnMap[it] ?: error("invalid column") }
        val noKeyColumns = if (keyColumns.size < columns.size) columns.filter { !keyColumns.contains(it) } else emptyList()
        if (rs.isBeforeFirst) hasNext = rs.next()
        var counter = 0
        while(hasNext && lrs.next()) {
            MDC.put("counter", counter++.toString())
            val result = compareByColumns(rs, lrs, keyColumns)
            when {
                result < 0 -> {
                    LOG.info("delete")
                    rs.deleteRow()
                    changes++
                }
                result > 0 -> {
                    LOG.info("insert")
                    rs.moveToInsertRow()
                    columns.filter { !it.isGeneratedColumn }.forEach { column ->
                        rs.updateObject(column.ordinal, lrs.getObject(column.ordinal - 1));
                    }
                    rs.insertRow()
                    changes++
                    rs.moveToCurrentRow()
                    hasNext = rs.next()
                }
                result == 0 -> {
                    if (!equalsByColumns(rs, lrs, noKeyColumns)) {
                        LOG.info("update")
                        val changedColumns = noKeyColumns.map { column ->
                            val rsVal = rs.getObject(column.ordinal)
                            val lrsVal = lrs.getObject(column.ordinal - 1)
                            Triple(rsVal, lrsVal, column)
                        }.toList()
                        if (changedColumns.isNotEmpty()) {
                            changedColumns.forEach {
                                rs.updateObject(it.third.ordinal, it.second)
                            }
                            rs.updateRow()
                            changes++
                        }
                    }
                    hasNext = rs.next()
                }
            }
        }
        if (!hasNext) {
            while (lrs.next()) {
                LOG.info("insert")
                rs.moveToInsertRow()
                columns.forEachIndexed { index, column ->
                    rs.updateObject(column.ordinal, lrs.getObject(index));
                }
                rs.insertRow()
                changes++
            }
        }
        total += lrs.size()
        fireProgressEvent()
        MDC.remove("counter")
    }

    private fun onClose() {
        while (hasNext) {
            LOG.info("delete")
            rs.deleteRow()
            changes++
            hasNext = rs.next()
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
