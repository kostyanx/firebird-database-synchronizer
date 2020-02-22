package ru.catcab.tool.database.synchronizer.util

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.util.DriverDataSource
import org.jetbrains.annotations.NotNull
import ru.kostyanx.utils.Config
import java.sql.ResultSet
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

object DbUtil {
    private const val TRANSACTION_READ_COMMITTED = "TRANSACTION_READ_COMMITTED"

    private val poolCounter = AtomicInteger()

    private fun nextPoolName(): String {
        return "pool-" + poolCounter.incrementAndGet()
    }

    fun Config.toFbPool(): HikariDataSource {

        val driver = "org.firebirdsql.jdbc.FBDriver"

        val host = asString("host")
        val port = asInt("port", 3050)
        val dbName = asString("dbname")

        val connectionString = "jdbc:firebird://$host:$port/$dbName"

        val newPoolName = asString("pool.name", nextPoolName())
        val pid = ProcessHandle.current().pid().toString()

        val connectionProperties = Properties().also {
            it["user"] = asString("username")
            it["password"] = asString("password")
            it["lc_ctype"] = asString("charset", "WIN1251")

            it["process_name"] = asString("process", "hikari_${pid}_$newPoolName")
            it["process_id"] = pid
            // TODO проверить, работает ли lock_timeout в таком контексте
            it["lock_timeout"] = asString("lock_timeout", "5")
        }


        val params = buildTransactionParams()

        val dds = DriverDataSource(connectionString, driver, connectionProperties.apply { putAll(params) }, null, null)

        val hikariConfig = buildConfig(newPoolName, dds)

        return HikariDataSource(hikariConfig)
    }

    private fun Config.buildConfig(
        newPoolName: @NotNull String,
        dds: DriverDataSource
    ): HikariConfig {
        val hikariConfig = HikariConfig().apply {
            poolName = newPoolName
            dataSource = dds
            minimumIdle = asInt("pool.min", 1)
            maximumPoolSize = asInt("pool.max", 5)
            idleTimeout = asLong("pool.idle_timeout", 30L) * 1000L
            maxLifetime = asLong("pool.max_lifetime", 300L) * 1000L
            isRegisterMbeans = true
            connectionTimeout = asLong("pool.conn_timeout", 30L) * 1000L
            isAutoCommit = enabled("autocommit")
        }
        return hikariConfig
    }

    private fun Config.buildTransactionParams(): Config {
        val params = part("params")
        val transParams =
            params.asString(TRANSACTION_READ_COMMITTED, "isc_tpb_read_committed,isc_tpb_rec_version").toMap().apply {
                if (!contains("isc_tpb_wait") && !contains("isc_tpb_nowait")) {
                    this["isc_tpb_wait"] = null
                }
                if (!contains("isc_tpb_lock_timeout") && !contains("isc_tpb_nowait")) {
                    this[",isc_tpb_lock_timeout"] = asString("lock_timeout", "5")
                }
                if (enabled("autocommit") && !contains("isc_tpb_autocommit")) {
                    this["isc_tpb_autocommit"] = null
                }
            }
        val transParamsStr = transParams.entries.joinToString(",") {
            it.run { if (value == null) key else "$key=$value" }
        }
        params[TRANSACTION_READ_COMMITTED] = transParamsStr
        println(params)
        return params
    }

    private fun String.toMap(): MutableMap<String, String?> {
        return split(',').associate {
            val kv = it.split(delimiters = *charArrayOf('='), limit = 2)
            kv[0] to if (kv.size > 1) kv[1] else null
        }.toMutableMap()
    }

    fun <T: Any> ResultSet.toList(rowMapper: (ResultSet) -> T): MutableList<T> {
        use {
            return generateSequence {
                if (it.next()) {
                    rowMapper(it)
                } else {
                    null
                }
            }.toMutableList()
        }
    }
}