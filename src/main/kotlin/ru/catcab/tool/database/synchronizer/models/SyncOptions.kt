package ru.catcab.tool.database.synchronizer.models

class SyncOptions(
    val deactivateIndices: Boolean = true,
    val deactivateTriggers: Boolean = true,
    val statListener: (SyncStats) -> Unit = {},
    val errorListener: (String, Throwable) -> Unit = {_,_ ->}
)