package ru.catcab.tool.database.synchronizer.module

import dagger.Module
import dagger.Provides
import org.slf4j.LoggerFactory
import ru.catcab.common.dagger.StartShutdownServiceLogger
import ru.kostyanx.utils.Config
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import javax.inject.Singleton

@Module
class CommonModule(
    private val config: Config
) {
    companion object { private val LOG = LoggerFactory.getLogger(DbModule::class.java)!! }

    @Provides @Singleton
    fun provideStartShutdownServiceLogger() = StartShutdownServiceLogger { LOG.info(it) }

    @Provides @Singleton
    fun provideConfig() = config

    @Provides @Singleton
    fun provideExecutorService(): ExecutorService = Executors.newSingleThreadExecutor()
}