package ru.catcab.tool.database.synchronizer.service

import org.slf4j.LoggerFactory
import ru.catcab.common.dagger.StartShutdownService
import ru.catcab.tool.database.synchronizer.models.SyncOptions
import ru.catcab.tool.database.synchronizer.models.UiConfig
import java.util.concurrent.ExecutorService
import kotlin.math.roundToLong

class CmdUiService(
    private val syncService: SyncService,
    private val startShutdownService: StartShutdownService,
    private val executor: ExecutorService,
    private val uiConfig: UiConfig
) : UiService {

    companion object {
        val LOG = LoggerFactory.getLogger(CmdUiService::class.java)
    }

    override fun start() {
        executor.submit {
            val start = System.currentTimeMillis()
            val tableMetas = syncService.getTableMetas()
            LOG.info("content to sync:")
            tableMetas.forEach { meta ->
                val table = meta.table.name
                val pkColumns = meta.primaryKey?.columns
                val pk = pkColumns?.joinToString(",")
                val indices = meta.indicies.filter { it.columns != pkColumns }
                    .joinToString(" ") { it.columns.joinToString(",") }
                val triggers = meta.triggers.joinToString { it.name }
                println("$table | $pk | $indices | $triggers")
            }
            syncService.sync(tableMetas, SyncOptions(
                deactivateIndices = "--no-deactivate-indices" !in uiConfig.args && "-ndi" !in uiConfig.args,
                deactivateTriggers = true,
                statListener = { stat ->
                    val time = ((System.currentTimeMillis() - start) / 1000.0).roundToLong()
                    if (stat.metrics.isEmpty()) { println() }
                    print("${stat.table}: ${stat.metrics} | ${time / 60}m ${time % 60}s\r")
                },
                errorListener = { table, error ->
                    println()
                    print("$table: *** ERROR ***: $error")
                }
            ))
            println()
            val time = ((System.currentTimeMillis() - start) / 1000.0).roundToLong()
            println("sync completed: ${time / 60}m ${time % 60}s")
        }.get()
        startShutdownService.shutdown()
    }

    override fun shutdown() {
        executor.shutdown()
    }
}
