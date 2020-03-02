package ru.catcab.tool.database.synchronizer.models

import java.sql.DatabaseMetaData.columnNoNulls
import java.sql.DatabaseMetaData.columnNullable
import java.sql.ResultSet

class Column(
    val catalog: String?,
    val schema: String?,
    val table: String,
    val name: String,
    val dataType: Int,
    val typeName: String,
    val size: Int,
    val decimalDigits: Int,
    val numPrecRadix: Int,
    val nullable: Boolean,
    val remarks: String?,
    val def: String?,
    val charOctetLength: Int,
    val ordinal: Int,
    val isNullable: Boolean,
    val scopeCatalog: String?,
    val scopeSchema: String?,
    val scopeTable: String?,
    val sourceDataType: Short?,
    val isAutoIncrement: Boolean,
    val isGeneratedColumn: Boolean
) {
    companion object {
        fun ResultSet.toColumn(ordinal: Int): Column {
            return Column(
                getString("TABLE_CAT"),
                getString("TABLE_SCHEM"),
                getString("TABLE_NAME"),
                getString("COLUMN_NAME"),
                getInt("DATA_TYPE"),
                getString("TYPE_NAME"),
                getInt("COLUMN_SIZE"),
                getInt("DECIMAL_DIGITS"),
                getInt("NUM_PREC_RADIX"),
                when(getInt("NULLABLE")) { columnNoNulls -> false; columnNullable -> true; else -> true },
                getString("REMARKS"),
                getString("COLUMN_DEF"),
                getInt("CHAR_OCTET_LENGTH"),
                ordinal,
                getString("IS_NULLABLE")?.toBoolean() ?: true,
                getString("SCOPE_CATALOG"),
                getString("SCOPE_SCHEMA"),
                getString("SCOPE_TABLE"),
                getShort("SOURCE_DATA_TYPE"),
                getString("IS_AUTOINCREMENT")?.toBoolean() ?: false,
                getString("IS_GENERATEDCOLUMN")?.toBoolean() ?: false
            )
        }

        fun ResultSet.toColumn(): Column {
            return toColumn(getInt("ORDINAL_POSITION"))
        }

        fun String.toBoolean(): Boolean? {
            return when(this) {
                "YES" -> true
                "NO" -> false
                else -> null
            }
        }
    }
}