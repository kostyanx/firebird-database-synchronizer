package ru.catcab.tool.database.synchronizer.util

import org.slf4j.Logger
import ru.catcab.utils.SynHolder
import ru.kostyanx.utils.NamedThreadFactory.named
import ru.kostyanx.utils.SafeExecutor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors.newSingleThreadExecutor
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

open class AsyncBatchExecutor<T>(
    private val logger: Logger,
    private val task: (T) -> Unit,
    private val onClose: (() -> Unit)?,
    private val lastTaskTimeout: Long,
    private val lastTaskUnit: TimeUnit
) : AutoCloseable {
    private val executor: ExecutorService = SafeExecutor(newSingleThreadExecutor(named("async-batch-inserter")), logger)
    private val lock = ReentrantLock(true)
    // for waiting previous task
    private var lastTask: Future<*>? = null
    @Volatile
    private var lastError: Throwable? = null

    fun submit(holder: SynHolder<out T>) {
        lastError?.also { throw it }
        // for retaining sequence of calls
        lock.withLock {
            lastTask?.get()
            lastTask = executor.submit(holder.createTask(::execute))
        }
    }

    private fun execute(subject: T) {
        lastError?.also {
            logger.error("an error occurred in the previous stage - skip task")
            return
        }
        try {
            task(subject)
        } catch (e: Throwable) {
            lastError = e;
            logger.error("error on execute batch:", e)
        }
    }

    override fun close() {
        if (onClose != null && lastError == null) {
            executor.execute(onClose)
        }
        executor.shutdown()
        try {
            executor.awaitTermination(lastTaskTimeout, lastTaskUnit)
        } catch (ignored: InterruptedException) {
        }
        lastError?.also { throw it }
    }
}