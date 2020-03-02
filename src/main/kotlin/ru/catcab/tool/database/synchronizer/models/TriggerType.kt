package ru.catcab.tool.database.synchronizer.models

import java.util.Map.entry
import java.util.Map.ofEntries

enum class TriggerType(val type: Long) {
    // TODO: add types for DDL
    BEFORE_INESRT(1),
    AFTER_INSERT(2),
    BEFORE_UPDATE(3),
    AFTER_UPDATE(4),
    BEFORE_DELETE(5),
    AFTER_DELETE(6),
    BEFORE_INSERT_OR_UPDATE(17),
    AFTER_INSERT_OR_UPDATE(18),
    BEFORE_INSERT_OR_DELETE(25),
    AFTER_INSERT_OR_DELETE(26),
    BEFORE_UPDATE_OR_DELETE(27),
    AFTER_UPDATE_OR_DELETE(28),
    BEFORE_INSERT_OR_UPDATE_OR_DELETE(113),
    AFTER_INSERT_OR_UPDATE_OR_DELETE(114),
    ON_CONNECT(8192),
    ON_DISCONNECT(8193),
    ON_TRASACTION_START(8194),
    ON_TRANSACTION_COMMIT(8195),
    ON_TRANSACTION_ROLLBACK(8196);

    companion object {
        private val typeMap = values().map { entry(it.type, it) }.toTypedArray().let { ofEntries(*it) } as Map<Long, TriggerType>

        fun Long.toTriggerType(): TriggerType {
            return typeMap[this] ?: throw IllegalArgumentException("unknown type: $this")
        }
    }
}
