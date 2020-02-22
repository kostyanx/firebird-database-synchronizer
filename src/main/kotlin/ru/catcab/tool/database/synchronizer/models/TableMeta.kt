package ru.catcab.tool.database.synchronizer.models

class TableMeta(
    val table: Table,
    val primaryKey: PrimaryKey?,
    val indicies: List<Index>
) {
}