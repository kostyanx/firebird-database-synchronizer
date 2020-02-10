package ru.catcab.tool.database.synchronizer.models

import java.sql.ResultSet

class Table(
    val catalog: String?,
    val schema: String?,
    val name: String,
    val type: String,
    val remarks: String?,
    val typeCat: String?,
    val typeSchem: String?,
    val typeName: String?,
    val selfReferencingColName: String?,
    val refGeneration: String?
) {
    companion object {
        fun ResultSet.toTable(): Table {
            return Table(
                getString("TABLE_CAT"),
                getString("TABLE_SCHEM"),
                getString("TABLE_NAME"),
                getString("TABLE_TYPE"),
                getString("REMARKS"),
                getString("TYPE_CAT"),
                getString("TYPE_SCHEM"),
                getString("TYPE_NAME"),
                getString("SELF_REFERENCING_COL_NAME"),
                getString("REF_GENERATION")
            )
        }
    }
}