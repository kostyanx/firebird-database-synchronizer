package ru.catcab.tool.database.synchronizer.util

object OS {
    val name = System.getProperty("os.name").toLowerCase()
    val isWindows = name.contains("win")
    val isMac = name.contains("mac")
    val isUnix = name.contains("nix") || name.contains("nux") || name.indexOf("aix") > 0
    val isSolaris = name.contains("sunos")
}