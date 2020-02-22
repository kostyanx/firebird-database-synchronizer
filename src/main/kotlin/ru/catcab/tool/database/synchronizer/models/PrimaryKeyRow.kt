package ru.catcab.tool.database.synchronizer.models

import java.sql.ResultSet

class PrimaryKeyRow(
    val catalog: String?,
    val schema: String?,
    val table: String,
    val column: String,
    val keySeq: Short,
    val pkName: String?
) {
    companion object {
        fun ResultSet.toPrimaryKeyRow(): PrimaryKeyRow {
            return PrimaryKeyRow(
                getString("TABLE_CAT"),
                getString("TABLE_SCHEM"),
                getString("TABLE_NAME"),
                getString("COLUMN_NAME"),
                getShort("KEY_SEQ"),
                getString("PK_NAME")
            )
        }
    }
}