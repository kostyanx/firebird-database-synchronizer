package ru.catcab.tool.database.synchronizer.module

import dagger.Module
import dagger.Provides
import ru.catcab.tool.database.synchronizer.util.DbUtil.toFbPool
import ru.kostyanx.dbproxy.DataSourceProxy
import ru.kostyanx.dbproxy.DataSourceProxyImpl
import ru.kostyanx.utils.Config
import javax.inject.Named
import javax.inject.Singleton

@Module
class DbModule {

    @Provides @Singleton
    @Named("database.source")
    fun provideSourceDatabase(config: Config): DataSourceProxy {
        val pool = config.part("database.source").toFbPool()
        return DataSourceProxyImpl(pool) { }
    }

    @Provides @Singleton
    @Named("database.destination")
    fun provideDestinationDatabase(config: Config): DataSourceProxy {
        val pool = config.part("database.destination").toFbPool()
        return DataSourceProxyImpl(pool) { }
    }

}