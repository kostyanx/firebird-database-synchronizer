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
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
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
                sync(srcConn, dstConn, tableMetas)
            }
        }
    }

    private fun sync(srcConn: ConnectionProxy, dstConn: ConnectionProxy, tableMetas: List<TableMeta>) {
//        syncData(srcConn, dstConn, tableMetas.single { it.table.name == "APIROUTECACHE" })
//        if (2 > 1) {
//            return
//        }
        tableMetas.filter { it.primaryKey == null }.forEach {
            LOG.warn("table without primary key will not be processed: {}", it.table.name)
        }
        tableMetas.filter { it.primaryKey != null }.forEach { tableMeta ->
            notifierService.fire("startSync", tableMeta.table.name)
            MDC.put("table", tableMeta.table.name)
            val triggersToDeactivate = tableMeta.triggers.filter { it.active && !it.isSystem }

            try {
                // deactivate triggers
                triggersToDeactivate.forEach { dstConn.deactivateTrigger(it.name) }
                syncData(srcConn, dstConn, tableMeta)
            } catch (e: Throwable) {
                notifierService.fire("syncError", tableMeta.table.name)
            } finally {
                // activate triggers
                triggersToDeactivate.forEach { dstConn.activateTrigger(it.name) }
                MDC.remove("table")
            }
        }
    }

    private fun syncData(srcConn: ConnectionProxy, dstConn: ConnectionProxy, tableMeta: TableMeta) {
        val table = tableMeta.table
        val sortColumns = tableMeta.primaryKey!!.columns
        val sql = "SELECT * FROM ${table.name} ORDER BY " + sortColumns.joinToString(",")
        dstConn.execute { conn: Connection ->
            val stmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            val writeRs = stmt.executeQuery()
            AsyncBatchComparator(writeRs, tableMeta, notifierService).use { asyncComparator ->
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
        return getColumns(null, null, table, null).toListIndexed { rs, num -> rs.toColumn(num + 1) }
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