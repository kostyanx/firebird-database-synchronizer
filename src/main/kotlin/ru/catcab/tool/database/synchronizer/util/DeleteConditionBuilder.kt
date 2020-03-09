package ru.catcab.tool.database.synchronizer.util

import org.antlr.v4.runtime.misc.Triple
import ru.catcab.tool.database.synchronizer.models.ShortConditons
import ru.kostyanx.utils.KostyanxUtil.q

class DeleteConditionBuilder<T>(
    private val key: String,
    private val maxMissedForNotIn: Long,
    private val stopParamsCount: Long,
    private val idOf: (T) -> Long
) {
    private val conditions: MutableList<String> = ArrayList()
    private val args: MutableList<Long> = ArrayList()
    private val smallGroups: MutableList<Long> = ArrayList()
    private var firstOfFirst: Long = 0
    private var lastOfLast: Long = 0
    private var lastInPrevList: Long = 0
    var totalSize: Long = 0

    fun addAndCheck(events: List<T>): Boolean {
        if (events.isEmpty()) return isFull()
        val first = idOf(events[0])
        val size = events.size
        lastOfLast = idOf(events[size - 1])
        val last = lastOfLast

        if (firstOfFirst == 0L) {
            firstOfFirst = first
            lastInPrevList = firstOfFirst
        }

        totalSize += size.toLong()
        if (lastOfLast - firstOfFirst + 1 == totalSize) {
            return true
        }

        val result = makeShortDeleteCondition(events, lastInPrevList, maxMissedForNotIn)
        if (result != null) {
            conditions.addAll(result.a)
            args.addAll(result.b)
            smallGroups.addAll(result.c)
        } else {
            conditions.add("(" + key + " < ? OR " + key + " > ? OR " + key + " IN  (" + q(events) + ")" + ")")
            args.add(first)
            args.add(last)
            events.forEach{ args.add(idOf(it)) }
        }
        lastInPrevList = last
        return isFull()
    }

    fun isFull() = smallGroups.size + args.size >= stopParamsCount

    private fun makeShortDeleteCondition(events: List<T>, lastInPrevList: Long, maxMissedForNotIn: Long): Triple<List<String>, List<Long>, List<Long>>? {
        val conditions: MutableList<String> = ArrayList()
        val args: MutableList<Long> = ArrayList()
        val smallGroups: MutableList<Long> = ArrayList()
        val limit = events.size / 2
        var prev = lastInPrevList
        var currArgsCount: Long = 0
        for (row in events) {
            val id = idOf(row)
            if (id - prev > 1) {
                val missed = id - prev - 1
                currArgsCount += if (missed > maxMissedForNotIn) 2 else missed
                if (currArgsCount < limit) {
                    if (missed > maxMissedForNotIn) { // if a lot of missed indexes collect to (KEY < ? or KEY > ?)
                        conditions.add("($key < ? or $key > ?)")
                        args.add(prev + 1)
                        args.add(id - 1)
                        currArgsCount += 2
                    } else { // collect indexes to for 'KEY not in (...)' condition
                        for (j in prev + 1 until id) {
                            smallGroups.add(j)
                        }
                        currArgsCount += missed
                    }
                } else {
                    return null
                }
            }
            prev = id
        }
        return Triple(conditions, args, smallGroups)
    }

    fun generateConditions(): ShortConditons {
        if (lastOfLast - firstOfFirst + 1 == totalSize) {
            return ShortConditons(
                "$key >= ? AND $key <= ?",
                listOf(firstOfFirst, lastOfLast)
            )
        }
        if (smallGroups.isNotEmpty()) {
            if (smallGroups.size <= 3) {
                repeat(smallGroups.size) { conditions.add("$key != ?") }
            } else {
                conditions.add(key + " NOT IN (${q(smallGroups)})")
            }
            args.addAll(smallGroups)
        }
        val sb = StringBuilder()
        conditions.forEach {
            sb.append(it).append(" AND ")
        }
        sb.append(key).append(" >= ? AND ").append(key).append(" <= ?")
        args.add(firstOfFirst) // >= first
        args.add(lastOfLast) // <= last
        return ShortConditons(sb.toString(), args)
    }

    fun reset() {
        firstOfFirst = 0
        lastOfLast = 0
        lastInPrevList = 0
        totalSize = 0
        conditions.clear()
        args.clear()
        smallGroups.clear()
    }
}