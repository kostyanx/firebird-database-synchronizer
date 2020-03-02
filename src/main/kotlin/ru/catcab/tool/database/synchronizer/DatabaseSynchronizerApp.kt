package ru.catcab.tool.database.synchronizer

import dagger.Component
import ru.catcab.common.dagger.StartShutdownService
import ru.catcab.tool.database.synchronizer.module.CommonModule
import ru.catcab.tool.database.synchronizer.module.DbModule
import ru.catcab.tool.database.synchronizer.service.SyncService
import ru.catcab.tool.database.synchronizer.service.UiService
import javax.inject.Singleton

@Singleton
@Component(modules = [CommonModule::class, DbModule::class])
interface DatabaseSynchronizerApp {
    fun syncService(): SyncService
    fun uiService(): UiService
    fun startShutdownService(): StartShutdownService
}
