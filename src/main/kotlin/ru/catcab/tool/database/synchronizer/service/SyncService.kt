package ru.catcab.tool.database.synchronizer.service

import org.slf4j.LoggerFactory
import ru.catcab.common.dagger.StartShutdownHandler
import ru.catcab.tool.database.synchronizer.models.Index
import ru.catcab.tool.database.synchronizer.models.Index.Companion.toIndexList
import ru.catcab.tool.database.synchronizer.models.IndexRow.Companion.toIndexRow
import ru.catcab.tool.database.synchronizer.models.PrimaryKey
import ru.catcab.tool.database.synchronizer.models.PrimaryKey.Companion.toPrimaryKey
import ru.catcab.tool.database.synchronizer.models.PrimaryKeyRow.Companion.toPrimaryKeyRow
import ru.catcab.tool.database.synchronizer.models.Table
import ru.catcab.tool.database.synchronizer.models.Table.Companion.toTable
import ru.catcab.tool.database.synchronizer.models.TableMeta
import ru.catcab.tool.database.synchronizer.util.DbUtil.toList
import ru.kostyanx.dbproxy.DataSourceProxy
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
    }

    private fun prepareSync() {
        srcDb.executeRawUpdate { conn ->
            val metaData = conn.metaData
            val tables = metaData.getTables()
            tables.map { it.name }.forEach(::println)
            val metas = metaData.getTableMetas();
            metas
                .map { "${it.table.name} pk: ${it.primaryKey?.columns} indicies: ${it.indicies.map { it.columns }}" }
                .forEach(::println)
        }
    }

    private fun DatabaseMetaData.getTableMetas(): List<TableMeta> {
        return getTables().map { table ->
            TableMeta(
                table,
                getPrimaryKey(table.name),
                getIndicies(table.name)
            )
        }
    }

    private fun DatabaseMetaData.getTables(): List<Table> {
        return getTables(null, null, null, arrayOf("TABLE")).toList { it.toTable() }
    }

    private fun DatabaseMetaData.getIndicies(table: String): List<Index> {
        return getIndexInfo(null, null, table, false, true).toList { it.toIndexRow() }.toIndexList()
    }

    private fun DatabaseMetaData.getPrimaryKey(table: String): PrimaryKey? {
        return getPrimaryKeys(null, null, table).toList { it.toPrimaryKeyRow() }.toPrimaryKey()
    }

    override fun shutdown() {
        srcDb.close()
        dstDb.close()
    }

    override fun start() {
        prepareSync()
    }
}