package ru.catcab.tool.database.synchronizer.service

import org.slf4j.LoggerFactory
import ru.catcab.common.dagger.StartShutdownHandler
import ru.catcab.tool.database.synchronizer.models.Table
import ru.catcab.tool.database.synchronizer.models.Table.Companion.toTable
import ru.catcab.tool.database.synchronizer.util.DbUtil.toList
import ru.kostyanx.dbproxy.DataSourceProxy
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
        val log = LoggerFactory.getLogger(SyncService::class.java)
    }

    fun prepareSync() {
        val tables = getTables()
        tables.map { it.name }.forEach(::println)
    }

    private fun getTables(): List<Table> {
        return srcDb.executeRawQuery {
            it.metaData.getTables(null, null, null, arrayOf("TABLE")).toList { it.toTable() }
        }
    }

    override fun shutdown() {
        srcDb.close()
        dstDb.close()
    }

    override fun start() {
        prepareSync()
    }
}