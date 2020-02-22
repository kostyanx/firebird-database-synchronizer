package ru.catcab.tool.database.synchronizer.models

import java.sql.ResultSet

class IndexRow(
    val catalog: String?,
    val schema: String?,
    val table: String,
    val nonUnique: Boolean,
    val qualifier: String?,
    val name: String?,
    val type: Short,
    val ordinal: Short,
    val column: String?,
    val ascDesc: String?,
    val cardinality: Long,
    val pages: Long,
    val filterCondition: String?
) {
    companion object {
        fun ResultSet.toIndexRow(): IndexRow {
            return IndexRow(
                getString("TABLE_CAT"),
                getString("TABLE_SCHEM"),
                getString("TABLE_NAME"),
                getBoolean("NON_UNIQUE"),
                getString("INDEX_QUALIFIER"),
                getString("INDEX_NAME"),
                getShort("TYPE"),
                getShort("ORDINAL_POSITION"),
                getString("COLUMN_NAME"),
                getString("ASC_OR_DESC"),
                getLong("CARDINALITY"),
                getLong("PAGES"),
                getString("FILTER_CONDITION")
            )
        }
    }
}