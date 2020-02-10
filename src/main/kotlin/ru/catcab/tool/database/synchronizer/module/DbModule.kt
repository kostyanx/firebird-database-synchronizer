package ru.catcab.tool.database.synchronizer.module

import dagger.Module
import dagger.Provides
import org.slf4j.LoggerFactory
import ru.catcab.common.dagger.StartShutdownServiceLogger
import ru.catcab.tool.database.synchronizer.util.DbUtil.toFbPool
import ru.kostyanx.dbproxy.DataSourceProxy
import ru.kostyanx.dbproxy.DataSourceProxyImpl
import ru.kostyanx.utils.Config
import javax.inject.Named
import javax.inject.Singleton

@Module
class DbModule(
    val config: Config
) {

    companion object {
        val LOG = LoggerFactory.getLogger(DbModule::class.java)
    }

    @Provides
    fun provideStartShutdownServiceLogger(): StartShutdownServiceLogger {
        return StartShutdownServiceLogger { LOG.info(it) }
    }

    @Provides
    @Singleton
    @Named("database.source")
    fun provideSourceDatabase(): DataSourceProxy {
        val pool = config.part("database.source").toFbPool()
        return DataSourceProxyImpl(pool) { }
    }

    @Provides @Singleton
    @Named("database.destination")
    fun provideDestinationDatabase(): DataSourceProxy {
        val pool = config.part("database.destination").toFbPool()
        return DataSourceProxyImpl(pool) { }
    }

}