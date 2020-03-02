package ru.catcab.tool.database.synchronizer.service

import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class NotifierService @Inject constructor() {
    val listeners = ConcurrentHashMap<String, MutableList<(Any) -> Unit>>()

    fun subscribe(event: String, listener: (Any) -> Unit) {
        listeners.computeIfAbsent(event) { mutableListOf() } += listener
    }

    fun fire(event: String, data: Any) {
        listeners[event]?.forEach { listener ->
            listener(data)
        }
    }
}