package ru.catcab.tool.database.synchronizer.models

class PrimaryKey(
    val catalog: String?,
    val schema: String?,
    val table: String,
    val columns: List<String>,
    val pkName: String?
) {
    companion object {

        fun MutableList<PrimaryKeyRow>.toPrimaryKey(): PrimaryKey? {
            if (isEmpty()) {
                return null
            }
            sortBy { it.keySeq }
            val row = this[0]
            val columns =
                if (size == 1) listOf(row.column) else makeColumnList()
            return PrimaryKey(
                row.catalog,
                row.schema,
                row.table,
                columns,
                row.pkName
            )
        }

        private fun List<PrimaryKeyRow>.makeColumnList() =
            sortedBy { it.keySeq }.map { it.column }.toList()
    }
}