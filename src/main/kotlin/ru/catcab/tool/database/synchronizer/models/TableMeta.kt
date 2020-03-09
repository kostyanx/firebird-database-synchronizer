package ru.catcab.tool.database.synchronizer.models

class TableMeta(
    val table: Table,
    val columns: List<Column>,
    val primaryKey: PrimaryKey?,
    val indicies: List<Index>,
    val triggers: List<Trigger>,
    val foreignKeys: List<String>
) {
}