package ru.catcab.tool.database.synchronizer

import org.slf4j.LoggerFactory
import ru.catcab.tool.database.synchronizer.module.DbModule
import ru.catcab.tool.database.synchronizer.util.OS
import ru.kostyanx.utils.Config
import java.io.File
import java.net.URLClassLoader
import java.util.jar.Manifest

object DatabaseSynchronizerMain {
    private val LOG = LoggerFactory.getLogger(DatabaseSynchronizerMain::class.java)!!

    @JvmStatic
    fun start(args: Array<String>) {
        LOG.info("start application")

        val config = Config.init("catcab-database-synchronizer.properties", DatabaseSynchronizer::class.java)

        val app = DaggerDatabaseSynchronizerApp.builder()
            .dbModule(DbModule(config))
            .build()

        app.startShutdownService().scanApp(app).start()
    }
}

fun main(args: Array<String>) {
    val excludeLib = when {
        OS.isWindows -> "org.eclipse.swt.gtk.linux"
        OS.isUnix -> "org.eclipse.swt.win32.win32"
        else -> "org.eclispe.swt.unknown"
    }

    val classPath = ArrayList<String>()

    DatabaseSynchronizerMain::class.java.getResourceAsStream("/META-INF/MANIFEST.MF")?.use { inStream ->
        Manifest(inStream).mainAttributes?.getValue("Class-Path")?.let { cp ->
            classPath += cp.split(" ")
        }
    }

    classPath += System.getProperty("java.class.path").split(File.pathSeparator)

    val urls = classPath.filter { !it.contains(excludeLib, false) }.map { File(it).toURI().toURL() }

    val topClassLoader = ClassLoader.getPlatformClassLoader().let { it.parent ?: it }

    val classLoader = URLClassLoader(urls.toTypedArray(), topClassLoader)

    Thread.currentThread().contextClassLoader = classLoader

    classLoader.loadClass(DatabaseSynchronizerMain::class.java.name)
        .getMethod("start", Array<String>::class.java)
        .invoke(null, args)
}