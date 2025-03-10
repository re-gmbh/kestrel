package com.cultureamp.eventsourcing

import arrow.core.NonEmptyList

class AsyncEventProcessorMonitor<M: EventMetadata>(
    private val asyncEventProcessors: NonEmptyList<AsyncEventProcessor<M>>,
    private val metrics: (Lag) -> Unit
) {
    fun run() {
        val lags = asyncEventProcessors.map {
            val bookmarkSequence = it.bookmarkStore.bookmarkFor(it.bookmarkName).sequence
            val lastSequence = it.eventSource.lastSequence(it.sequencedEventProcessor.domainEventClasses())
            Lag(
                name = it.bookmarkName,
                bookmarkSequence = bookmarkSequence,
                lastSequence = lastSequence
            )

        }

        lags.forEach {
            metrics(it)
        }
    }
}

data class Lag(val name: BookmarkName, val bookmarkSequence: Long, val lastSequence: Long) {
    val lag: Long = lastSequence - bookmarkSequence
}
