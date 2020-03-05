package ru.catcab.tool.database.synchronizer.service

import org.slf4j.LoggerFactory
import org.slf4j.MDC
import ru.catcab.common.dagger.StartShutdownHandler
import ru.catcab.tool.database.synchronizer.models.*
import ru.catcab.tool.database.synchronizer.models.Column.Companion.toColumn
import ru.catcab.tool.database.synchronizer.models.Index.Companion.toIndexList
import ru.catcab.tool.database.synchronizer.models.IndexRow.Companion.toIndexRow
import ru.catcab.tool.database.synchronizer.models.PrimaryKey.Companion.toPrimaryKey
import ru.catcab.tool.database.synchronizer.models.PrimaryKeyRow.Companion.toPrimaryKeyRow
import ru.catcab.tool.database.synchronizer.models.Table.Companion.toTable
import ru.catcab.tool.database.synchronizer.models.Trigger.Companion.toTrigger
import ru.catcab.tool.database.synchronizer.util.AsyncBatchComparator
import ru.catcab.tool.database.synchronizer.util.DbUtil.toList
import ru.catcab.tool.database.synchronizer.util.DbUtil.toListIndexed
import ru.catcab.utils.SynHolder
import ru.kostyanx.database.LocalResultSet
import ru.kostyanx.dbproxy.ConnectionProxy
import ru.kostyanx.dbproxy.DataSourceProxy
import ru.kostyanx.dbproxy.DatabaseRawQuery
import ru.kostyanx.dbproxy.TypedResultSetProcessor
import java.sql.DatabaseMetaData
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

@Singleton
class SyncService @Inject constructor(
    @Named("database.source")
    private val srcDb: DataSourceProxy,
    @Named("database.destination")
    private val dstDb: DataSourceProxy,
    private val notifierService: NotifierService
) : StartShutdownHandler {
    companion object {
        private val LOG = LoggerFactory.getLogger(SyncService::class.java)
        private const val SELECT_TRIGGERS_SQL = "SELECT * FROM RDB\$TRIGGERS WHERE RDB\$RELATION_NAME = ?"
        private const val ACTIVATE_TRIGGER_SQL = "ALTER TRIGGER {trigger} ACTIVE"
        private const val DEACTIVATE_TRIGGER_SQL = "ALTER TRIGGER {trigger} INACTIVE"
    }

    fun getTableMetas() = srcDb.executeQueryAc { it.getTableMetas() }!!

    fun ConnectionProxy.getTableMetas(): List<TableMeta> {
        Exception().stackTrace = arrayOf()
        return execute(DatabaseRawQuery { conn ->
            val metaData = conn.metaData
            metaData.getTables().map { table ->
                TableMeta(
                    table,
                    metaData.getColumns(table.name),
                    metaData.getPrimaryKey(table.name),
                    metaData.getIndicies(table.name),
                    getTriggers(table.name)
                )
            }
        })
    }

    fun sync() {
        val tableMetas = srcDb.executeQuery { it.getTableMetas() }
        sync(tableMetas)
    }

    fun sync(tableMetas: List<TableMeta>) {
        srcDb.connectionProxy.use { srcConn ->
            dstDb.connectionProxy.use { dstConn ->
                dstDb.connectionProxy.use { dstConnUpdate ->
                    sync(srcConn, dstConn, dstConnUpdate, tableMetas)
                }

            }
        }
    }

    private fun sync(srcConn: ConnectionProxy, dstConn: ConnectionProxy, dstConnUpdate: ConnectionProxy, tableMetas: List<TableMeta>) {
        tableMetas.filter { it.primaryKey == null }.forEach {
            LOG.warn("table without primary key will not be processed: {}", it.table.name)
        }
        tableMetas.filter { it.primaryKey != null }.forEach { tableMeta ->
            MDC.put("table", tableMeta.table.name)
            notifierService.fire("startSync", tableMeta.table.name)

            val triggersToDeactivate = tableMeta.triggers.filter { it.active && !it.isSystem }
            try {
                // deactivate triggers
                triggersToDeactivate.forEach { dstConn.deactivateTrigger(it.name) }
                syncData(srcConn, dstConn, dstConnUpdate, tableMeta)
            } catch (e: Throwable) {
                notifierService.fire("syncError", tableMeta.table.name)
            } finally {
                // activate triggers
                triggersToDeactivate.forEach { dstConn.activateTrigger(it.name) }
                MDC.remove("table")
            }
        }
    }

    private fun syncData(srcConn: ConnectionProxy, dstConn: ConnectionProxy, dstConnUpdate: ConnectionProxy, tableMeta: TableMeta) {
        val table = tableMeta.table
        val sortColumns = tableMeta.primaryKey!!.columns
        val sql = "SELECT * FROM ${table.name} ORDER BY " + sortColumns.joinToString(",")
        dstConn.executeQuery(sql, listOf<Any?>()) { writeRs ->
            AsyncBatchComparator(tableMeta, writeRs, dstConnUpdate, notifierService).use { asyncComparator ->
                srcConn.executeQuery(sql, listOf<Any?>()) { readRs ->
                    val batchSize = 10000
                    var read = batchSize
                    while (read == batchSize)
                    do {
                        val holder = SynHolder.create {
                            LocalResultSet().also { it.fetch(readRs, batchSize) }
                        }
                        asyncComparator.submit(holder)
                        read = holder.get().size()
                    } while (read == batchSize)
                }
            }
        }
    }

    private fun DatabaseMetaData.getTables(): List<Table> {
        return getTables(null, null, null, arrayOf("TABLE")).toList { it.toTable() }
    }

    private fun DatabaseMetaData.getColumns(table: String): List<Column> {
        return getColumns(null, null, table, null).toListIndexed { rs, num -> rs.toColumn(num + 1).also { if (it.isGeneratedColumn) LOG.info("$table.${it.name}") } }
    }

    private fun DatabaseMetaData.getIndicies(table: String): List<Index> {
        return getIndexInfo(null, null, table, false, true).toList { it.toIndexRow() }.toIndexList()
    }

    private fun DatabaseMetaData.getPrimaryKey(table: String): PrimaryKey? {
        return getPrimaryKeys(null, null, table).toList { it.toPrimaryKeyRow() }.toPrimaryKey()
    }

    private fun ConnectionProxy.getTriggers(table: String): List<Trigger> {
        return executeQuery(SELECT_TRIGGERS_SQL, listOf(table), TypedResultSetProcessor { rs ->
            rs.toList { it.toTrigger() }
        })
    }

    private fun ConnectionProxy.activateTrigger(name: String) {
        LOG.info("activate trigger: {}", name)
        execute(ACTIVATE_TRIGGER_SQL.replace("{trigger}", name))
    }

    private fun ConnectionProxy.deactivateTrigger(name: String) {
        LOG.info("deactivate trigger: {}", name)
        execute(DEACTIVATE_TRIGGER_SQL.replace("{trigger}", name))
    }

    override fun shutdown() {
        srcDb.close()
        dstDb.close()
    }

    override fun start() {

    }
}