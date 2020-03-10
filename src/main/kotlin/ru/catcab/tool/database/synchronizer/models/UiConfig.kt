package ru.catcab.tool.database.synchronizer.models

import ru.kostyanx.utils.Config
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class UiConfig @Inject constructor(
    config: Config,
    val args: Array<String>
) {
    val type: UiType = when(config.asString("ui.type", "cmd")) {
        "swt" -> UiType.SWT
        "cmd" -> UiType.CMD
        else -> UiType.CMD
    }
    enum class UiType {
        SWT, CMD
    }
}