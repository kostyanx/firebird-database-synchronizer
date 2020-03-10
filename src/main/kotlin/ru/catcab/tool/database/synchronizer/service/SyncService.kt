package ru.catcab.tool.database.synchronizer.service

import org.intellij.lang.annotations.Language
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
    private val dstDb: DataSourceProxy
) : StartShutdownHandler {
    companion object {
        private val LOG = LoggerFactory.getLogger(SyncService::class.java)
        @Language("GenericSQL")
        private const val SELECT_TRIGGERS_SQL = "SELECT * FROM RDB\$TRIGGERS WHERE RDB\$RELATION_NAME = ?"
        @Language("GenericSQL")
        private const val ACTIVATE_TRIGGER_SQL = "ALTER TRIGGER {trigger} ACTIVE"
        @Language("GenericSQL")
        private const val DEACTIVATE_TRIGGER_SQL = "ALTER TRIGGER {trigger} INACTIVE"
        @Language("GenericSQL")
        private const val ACTIVATE_INDEX_SQL = "ALTER INDEX {index} ACTIVE"
        @Language("GenericSQL")
        private const val DEACTIVATE_INDEX_SQL = "ALTER INDEX {index} INACTIVE"
        @Language("GenericSQL")
        private const val SELECT_FOREIGN_KEYS_SQL = "SELECT rtrim(R.RDB\$CONSTRAINT_NAME) AS CONSTRAINT_NAME" +
                " FROM RDB\$RELATION_CONSTRAINTS R" +
                " WHERE (R.RDB\$CONSTRAINT_TYPE='FOREIGN KEY')"
        @Language("GenericSQL")
        private const val SELECT_GENERATORS_SQL = "SELECT rtrim(RDB\$GENERATOR_NAME) as GENERATOR_NAME FROM RDB\$GENERATORS WHERE RDB\$SYSTEM_FLAG = 0"
        @Language("GenericSQL")
        private const val GET_GENERATOR_COUNTER_SQL = "SELECT GEN_ID({generator}, 0) as COUNTER FROM RDB\$DATABASE"
        @Language("GenericSQL")
        private const val SET_GENERATOR_COUNTER_SQL = "SET GENERATOR {generator} TO {counter}"
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
                    getTriggers(table.name),
                    getForeignKeys()
                )
            }
        })
    }

    fun sync(syncOptions: SyncOptions) {
        val tableMetas = srcDb.executeQuery { it.getTableMetas() }
        sync(tableMetas, syncOptions)
    }

    fun sync(tableMetas: List<TableMeta>, syncOptions: SyncOptions) {
        srcDb.connectionProxy.use { srcConn ->
            dstDb.connectionProxy.use { dstConn ->
                dstDb.connectionProxy.use { dstConnUpdate ->
                    LOG.info("update generator values")
                    updateGenerators(srcConn, dstConnUpdate)
                    LOG.info("start data sync")
                    sync(srcConn, dstConn, dstConnUpdate, tableMetas, syncOptions)
                }
            }
        }
    }

    private fun sync(
        srcConn: ConnectionProxy,
        dstConn: ConnectionProxy,
        dstConnUpdate: ConnectionProxy,
        tableMetas: List<TableMeta>,
        syncOptions: SyncOptions
    ) {
        tableMetas.filter { it.primaryKey == null }.forEach {
            LOG.warn("table without primary key will not be processed: {}", it.table.name)
        }
        tableMetas.filter { it.primaryKey != null }.forEach { tableMeta ->
            val tableName = tableMeta.table.name
            MDC.put("table", tableName)
            val syncMetrics = SyncStats(System.currentTimeMillis(), tableName, "")
            syncOptions.statListener(syncMetrics)
            try {
                LOG.info("sync table: {}", tableName)
                syncTable(tableMeta, dstConn, srcConn, dstConnUpdate, syncOptions, syncMetrics)
            } catch (e: Throwable) {
                syncOptions.errorListener(tableName, e)
                LOG.error("sync table $tableName error:", e)
            } finally {
                MDC.remove("table")
            }
        }
    }

    private fun syncTable(
        tableMeta: TableMeta,
        dstConn: ConnectionProxy,
        srcConn: ConnectionProxy,
        dstConnUpdate: ConnectionProxy,
        syncOptions: SyncOptions,
        syncStats: SyncStats
    ) {
        val pk = tableMeta.primaryKey!!
        val indices = tableMeta.indicies.filter { it.name != null && it.columns != pk.columns && it.name !in tableMeta.foreignKeys }
        val triggers = tableMeta.triggers.filter { it.active && !it.isSystem }

        if (syncOptions.deactivateTriggers) deactivateTriggers(triggers, dstConn)
        if (syncOptions.deactivateIndices) deactivateIndices(indices, dstConn)
        try {
            syncData(srcConn, dstConn, dstConnUpdate, tableMeta, syncOptions, syncStats)
        } catch (e: Throwable) {
            LOG.error("sync table ${tableMeta.table.name} data error:", e)
            throw e
        } finally {
            if (syncOptions.deactivateIndices) activateIndices(indices, dstConn)
            if (syncOptions.deactivateTriggers) activateTriggers(triggers, dstConn)
        }
    }

    private fun syncData(
        srcConn: ConnectionProxy,
        dstConn: ConnectionProxy,
        dstConnUpdate: ConnectionProxy,
        tableMeta: TableMeta,
        syncOptions: SyncOptions,
        syncStats: SyncStats
    ) {
        val table = tableMeta.table
        val sortColumns = tableMeta.primaryKey!!.columns
        val sql = "SELECT * FROM ${table.name} ORDER BY " + sortColumns.joinToString(",")
        dstConn.executeQuery(sql, listOf<Any?>()) { writeRs ->
            AsyncBatchComparator(tableMeta, writeRs, dstConnUpdate, syncOptions, syncStats).use { asyncComparator ->
                srcConn.executeQuery(sql, listOf<Any?>()) { readRs ->
                    val batchSize = 10000
                    var read: Int
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

    private fun updateGenerators(srcConn: ConnectionProxy, dstConn: ConnectionProxy) {
        for (generator in srcConn.getGenerators()) {
            val counter = srcConn.getGeneratorValue(generator)
            LOG.debug("set generator {} counter to {}", generator, counter)
            dstConn.setGeneratorValue(generator, counter)
        }
    }

    private fun ConnectionProxy.getForeignKeys(): List<String> {
        return executeQueryList(SELECT_FOREIGN_KEYS_SQL, listOf<Any>()) { it.getString(1) }
    }

    private fun ConnectionProxy.getGenerators(): List<String> {
        return executeQueryList(SELECT_GENERATORS_SQL, listOf<Any>()) { it.getString(1) }
    }

    private fun ConnectionProxy.getGeneratorValue(name: String): Long {
        return executeQuery(GET_GENERATOR_COUNTER_SQL.replace("{generator}", name), listOf<Any>(), TypedResultSetProcessor { rs ->
            rs.next()
            return@TypedResultSetProcessor rs.getLong(1)
        })
    }

    private fun ConnectionProxy.setGeneratorValue(name: String, counter: Long) {
        val sql = SET_GENERATOR_COUNTER_SQL.replace("{generator}", name).replace("{counter}", counter.toString())
        executeUpdate(sql, listOf<Any>())
    }

    private fun DatabaseMetaData.getTables(): List<Table> {
        return getTables(null, null, null, arrayOf("TABLE")).toList { it.toTable() }
    }

    private fun DatabaseMetaData.getColumns(table: String): List<Column> {
        return getColumns(null, null, table, null).toListIndexed { rs, num ->
            rs.toColumn(num + 1)
        }
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

    private fun activateIndices(
        indices: List<Index>,
        dstConn: ConnectionProxy
    ) {
        indices.forEach { index ->
            try {
                LOG.debug("activate index: {}", index.name)
                dstConn.activateIndex(index.name!!)
            } catch (e: Throwable) {
                LOG.debug("can't activate index: {}", index, e)
            }
        }
    }

    private fun deactivateIndices(
        indices: List<Index>,
        dstConn: ConnectionProxy
    ) {
        indices.forEach { index ->
            try {
                LOG.debug("deactivate index: {}", index.name)
                dstConn.deactivateIndex(index.name!!)
            } catch (e: Throwable) {
                LOG.debug("can't deactivate index: {}", index, e)
            }
        }
    }

    private fun activateTriggers(
        triggers: List<Trigger>,
        dstConn: ConnectionProxy
    ) {
        triggers.forEach {
            LOG.debug("deactivate trigger: {}", it.name)
            dstConn.activateTrigger(it.name)
        }
    }

    private fun deactivateTriggers(
        triggers: List<Trigger>,
        dstConn: ConnectionProxy
    ) {
        triggers.forEach {
            LOG.debug("deactivate trigger: {}", it.name)
            dstConn.deactivateTrigger(it.name)
        }
    }


    private fun ConnectionProxy.activateTrigger(name: String) {
        execute(ACTIVATE_TRIGGER_SQL.replace("{trigger}", name))
    }

    private fun ConnectionProxy.deactivateTrigger(name: String) {
        execute(DEACTIVATE_TRIGGER_SQL.replace("{trigger}", name))
    }

    private fun ConnectionProxy.activateIndex(name: String) {
        execute(ACTIVATE_INDEX_SQL.replace("{index}", name))
    }

    private fun ConnectionProxy.deactivateIndex(name: String) {
        execute(DEACTIVATE_INDEX_SQL.replace("{index}", name))
    }

    override fun shutdown() {
        srcDb.close()
        dstDb.close()
    }

    override fun start() {

    }
}