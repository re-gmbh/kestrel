package com.cultureamp.eventsourcing

import com.cultureamp.common.Action
import kotlin.random.Random

@JvmInline
value class BookmarkName(private val name: String) {
    override fun toString(): String = name
}

interface BookmarkedEventProcessor<M : EventMetadata> {
    val bookmarkStore: BookmarkStore
    val bookmarkName: BookmarkName
    val sequencedEventProcessor: SequencedEventProcessor<M>

    companion object {
        fun <M : EventMetadata> from(
            bookmarkStore: BookmarkStore,
            bookmarkName: BookmarkName,
            eventProcessor: EventProcessor<M>
        ) = from(
            bookmarkStore,
            bookmarkName,
            SequencedEventProcessor.from(eventProcessor),
        )

        fun <M : EventMetadata> from(
            bookmarkStore: BookmarkStore,
            bookmarkName: BookmarkName,
            eventProcessor: SequencedEventProcessor<M>
        ) = object : BookmarkedEventProcessor<M> {
            override val bookmarkStore = bookmarkStore
            override val bookmarkName = bookmarkName
            override val sequencedEventProcessor = eventProcessor
        }
    }
}

interface AsyncEventProcessor<M : EventMetadata> : BookmarkedEventProcessor<M> {
    val eventSource: EventSource<M>
}

class BatchedAsyncEventProcessor<M : EventMetadata>(
    override val eventSource: EventSource<M>,
    override val bookmarkStore: BookmarkStore,
    override val bookmarkName: BookmarkName,
    override val sequencedEventProcessor: SequencedEventProcessor<M>,
    private val batchSize: Int = 1000,
    private val startLog: (Bookmark) -> Unit = { bookmark ->
        System.out.println("Polling for events for ${bookmark.name} from sequence ${bookmark.sequence}")
    },
    private val endLog: (Int, Bookmark) -> Unit = { count, bookmark ->
        if (count > 0 || Random.nextFloat() < 0.01) {
            System.out.println("Finished processing batch for ${bookmark.name}, $count events up to sequence ${bookmark.sequence}")
        }
    },
    private val upcasting: Boolean = true,
    private val stats: StatisticsCollector? = null,
) : AsyncEventProcessor<M> {

    constructor(
        eventSource: EventSource<M>,
        bookmarkStore: BookmarkStore,
        bookmarkName: BookmarkName,
        eventProcessor: EventProcessor<M>,
        batchSize: Int = 1000,
        startLog: (Bookmark) -> Unit = { bookmark ->
            System.out.println("Polling for events for ${bookmark.name} from sequence ${bookmark.sequence}")
        },
        endLog: (Int, Bookmark) -> Unit = { count, bookmark ->
            if (count > 0 || Random.nextFloat() < 0.01) {
                System.out.println("Finished processing batch for ${bookmark.name}, $count events up to sequence ${bookmark.sequence}")
            }
        },
        upcasting: Boolean = true,
        stats: StatisticsCollector? = null,
    ) : this(
        eventSource, bookmarkStore, bookmarkName, SequencedEventProcessor.from(eventProcessor), batchSize, startLog, endLog, upcasting, stats,
    )

    fun processOneBatch(): Action {
        val startBookmark = bookmarkStore.bookmarkFor(bookmarkName)

        startLog(startBookmark)

        val (count, finalBookmark) = eventSource.getAfter(startBookmark.sequence, sequencedEventProcessor.domainEventClasses(), batchSize).foldIndexed(
            0 to startBookmark,
        ) { index, _, sequencedEvent ->
            when (upcasting) {
                true -> {
                    val domainEvent = sequencedEvent.event.domainEvent
                    val upcastEvent = domainEvent::class.annotations.filterIsInstance<UpcastEvent>()
                    if (upcastEvent.size == 1) {
                        val newEvent = SequencedEvent(
                            Event(
                                id = sequencedEvent.event.id,
                                aggregateId = sequencedEvent.event.aggregateId,
                                aggregateSequence = sequencedEvent.event.aggregateSequence,
                                aggregateType = sequencedEvent.event.aggregateType,
                                createdAt = sequencedEvent.event.createdAt,
                                metadata = sequencedEvent.event.metadata,
                                domainEvent = upcastEvent.first().upcasting(domainEvent, sequencedEvent.event.metadata),
                            ),
                            sequencedEvent.sequence,
                        )
                        processEvent(newEvent)
                    } else {
                        processEvent(sequencedEvent)
                    }
                }
                false -> processEvent(sequencedEvent)
            }

            val updatedBookmark = startBookmark.copy(sequence = sequencedEvent.sequence)
            bookmarkStore.save(updatedBookmark)
            index + 1 to updatedBookmark
        }

        endLog(count, finalBookmark)

        return if (count >= batchSize) Action.Continue else Action.Wait
    }

    private fun processEvent(event: SequencedEvent<out M>) {
        stats?.let {
            val startTime = System.currentTimeMillis()
            sequencedEventProcessor.process(event)
            stats.eventProcessed(this, event, System.currentTimeMillis() - startTime)
        } ?: sequencedEventProcessor.process(event)
    }
}
