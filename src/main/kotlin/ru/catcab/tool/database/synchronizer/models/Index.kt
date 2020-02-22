package ru.catcab.tool.database.synchronizer.models

import ru.catcab.tool.database.synchronizer.models.Direction.Companion.toDirection

class Index(
    val catalog: String?,
    val schema: String?,
    val table: String,
    val nonUnique: Boolean,
    val qualifier: String?,
    val name: String?,
    val type: Short,
    val columns: List<String>,
    val direction: Direction?,
    val cardinality: Long,
    val pages: Long,
    val filterCondition: String?
) {
    companion object {
        fun MutableList<IndexRow>.toIndexList(): List<Index> {
            return groupBy { it.name }.values.map { subList ->
                with(subList as MutableList<IndexRow>) {
                    sortBy { it.ordinal }
                    val row = this[0]
                    val columns = map { it.column }.filterNotNull()
                    return@with Index(
                        row.catalog,
                        row.schema,
                        row.table,
                        row.nonUnique,
                        row.qualifier,
                        row.name,
                        row.type,
                        columns,
                        row.ascDesc?.toDirection(),
                        row.cardinality,
                        row.pages,
                        row.filterCondition
                    )
                }
            }
        }
    }
}