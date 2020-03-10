package ru.catcab.tool.database.synchronizer.module

import dagger.Module
import dagger.Provides
import ru.catcab.common.dagger.StartShutdownService
import ru.catcab.tool.database.synchronizer.models.UiConfig
import ru.catcab.tool.database.synchronizer.service.CmdUiService
import ru.catcab.tool.database.synchronizer.service.SwtUiService
import ru.catcab.tool.database.synchronizer.service.SyncService
import ru.catcab.tool.database.synchronizer.service.UiService
import java.util.concurrent.ExecutorService
import javax.inject.Singleton

@Module
class UiModule {

    @Provides @Singleton
    fun provideUiService(
        uiConfig: UiConfig,
        syncService: SyncService,
        startShutdownService: StartShutdownService,
        executor: ExecutorService
    ): UiService = when (uiConfig.type) {
        UiConfig.UiType.SWT -> SwtUiService(syncService, startShutdownService, executor)
        UiConfig.UiType.CMD -> CmdUiService(syncService, startShutdownService, executor, uiConfig)
    }
}