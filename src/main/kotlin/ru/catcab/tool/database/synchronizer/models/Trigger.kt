package ru.catcab.tool.database.synchronizer.models

import ru.catcab.tool.database.synchronizer.models.TriggerType.Companion.toTriggerType
import java.sql.ResultSet

class Trigger(
    val catalog: String?,
    val schema: String?,
    val name: String,
    val sequence: Short,
    val type: TriggerType,
    val active: Boolean,
    val isSystem: Boolean,
    val engineName: String?,
    val entryPoint: String?
) {
    companion object {
        fun ResultSet.toTrigger(): Trigger {
            return Trigger(
                null,
                getString("RDB\$RELATION_NAME")?.trim(),
                getString("RDB\$TRIGGER_NAME").trim(),
                getShort("RDB\$TRIGGER_SEQUENCE"),
                getLong("RDB\$TRIGGER_TYPE").toTriggerType(),
                getShort("RDB\$TRIGGER_INACTIVE") == 0.toShort(),
                getShort("RDB\$SYSTEM_FLAG") != 0.toShort(),
                getString("RDB\$ENGINE_NAME"),
                getString("RDB\$ENTRYPOINT")
            )
        }
    }
}