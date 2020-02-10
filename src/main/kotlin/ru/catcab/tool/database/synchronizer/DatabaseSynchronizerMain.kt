package ru.catcab.tool.database.synchronizer

import org.slf4j.LoggerFactory
import ru.catcab.tool.database.synchronizer.module.DbModule
import ru.kostyanx.utils.Config



fun main() {
    val log = LoggerFactory.getLogger(DatabaseSynchronizer::class.java)

    log.info("start application")
    val config = Config.init("catcab-database-synchronizer.properties", DatabaseSynchronizer::class.java)
    val app = DaggerDatabaseSynchronizerApp.builder()
        .dbModule(DbModule(config))
        .build()
    app.startShutdownService().scanApp(app).start()
}