package com.cultureamp.eventsourcing

import io.kotest.core.Tuple3
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.iterator.shouldHaveNext
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.shouldBe
import org.joda.time.DateTime
import java.util.*

class EventProcessorTest : DescribeSpec({
    val fooDomainEvent = FooEvent("bar")
    val fooEvent = Event(
        id = UUID.randomUUID(),
        aggregateId = UUID.randomUUID(),
        aggregateSequence = 1,
        aggregateType = "foo",
        createdAt = DateTime.now(),
        metadata = SpecificMetadata("specialField"),
        domainEvent = fooDomainEvent
    )
    val bazDomainEvent = BazEvent("quux")
    val bazEvent = Event(
        id = UUID.randomUUID(),
        aggregateId = UUID.randomUUID(),
        aggregateSequence = 1,
        aggregateType = "baz",
        createdAt = DateTime.now(),
        metadata = SpecificMetadata("specialField"),
        domainEvent = bazDomainEvent
    )

    describe("from((E, UUID) -> Any?)") {
        it("can process events with their aggregateIds") {
            val events = mutableMapOf<UUID, TestEvent>()

            class Projector {
                fun project(event: FooEvent, aggregateId: UUID) {
                    events[aggregateId] = event
                }
            }
            val eventProcessor = EventProcessor.from(Projector()::project)

            eventProcessor.process(fooEvent)

            events shouldContain (fooEvent.aggregateId to fooDomainEvent)
        }
    }

    describe("from((DomainEvent, UUID, EventMetadata, UUID) -> Any?)") {
        it("can process events with their aggregateIds, metadata and eventIds") {
            val events = mutableMapOf<UUID, Tuple3<TestEvent, EventMetadata, UUID>>()
            class ProjectorWithMetadata {
                fun project(event: FooEvent, aggregateId: UUID, metadata: SpecificMetadata, eventId: UUID) {
                    events[aggregateId] = Tuple3(event, metadata, eventId)
                }
            }

            val eventProcessor = EventProcessor.from(ProjectorWithMetadata()::project)

            eventProcessor.process(fooEvent)

            events shouldContain (fooEvent.aggregateId to Tuple3(fooDomainEvent, fooEvent.metadata, fooEvent.id))
        }
    }

    describe("from(DomainEventProcessor)") {
        it("can process events with their aggregateIds") {
            val events = mutableMapOf<UUID, TestEvent>()
            val projector = object : DomainEventProcessor<FooEvent> {
                override fun process(event: FooEvent, aggregateId: UUID) {
                    events[aggregateId] = event
                }
            }

            val eventProcessor = EventProcessor.from(projector)

            eventProcessor.process(fooEvent)

            events shouldContain (fooEvent.aggregateId to fooDomainEvent)
        }
    }

    describe("from(DomainEventProcessorWithMetadata)") {
        it("can process events with their aggregateIds, metadata and eventIds") {
            val events = mutableMapOf<UUID, Tuple3<TestEvent, EventMetadata, UUID>>()
            val projector = object : DomainEventProcessorWithMetadata<FooEvent, SpecificMetadata> {
                override fun process(event: FooEvent, aggregateId: UUID, metadata: SpecificMetadata, eventId: UUID) {
                    events[aggregateId] = Tuple3(event, metadata, eventId)
                }
            }

            val eventProcessor = EventProcessor.from(projector)

            eventProcessor.process(fooEvent)

            events shouldContain (fooEvent.aggregateId to Tuple3(fooDomainEvent, fooEvent.metadata, fooEvent.id))
        }
    }

    describe("EventListener#compose") {
        it("can combine three EventListeners into one") {
            val events = mutableListOf<Triple<UUID, DomainEvent, Any>>()
            val projector = object {
                fun project(event: FooEvent, aggregateId: UUID) {
                    events.add(Triple(aggregateId, event, this))
                }
            }
            val projectorWithMetadata = object {
                @Suppress("UNUSED_PARAMETER")
                fun project(event: BazEvent, aggregateId: UUID, metadata: SpecificMetadata, eventId: UUID) {
                    events.add(Triple(aggregateId, event, this))
                }
            }
            val projectorWithOverlap = object {
                fun project(event: FooEvent, aggregateId: UUID) {
                    events.add(Triple(aggregateId, event, this))
                }
            }

            val eventProcessor = EventProcessor.compose(
                EventProcessor.from(projector::project),
                EventProcessor.from(projectorWithMetadata::project),
                EventProcessor.from(projectorWithOverlap::project)
            )

            eventProcessor.process(fooEvent)
            eventProcessor.process(bazEvent)
            val it = events.iterator()
            it.shouldHaveNext()
            it.next() shouldBe Triple(fooEvent.aggregateId, fooDomainEvent, projector)
            it.shouldHaveNext()
            it.next() shouldBe Triple(fooEvent.aggregateId, fooDomainEvent, projectorWithOverlap)
            it.shouldHaveNext()
            it.next() shouldBe Triple(bazEvent.aggregateId, bazDomainEvent, projectorWithMetadata)
        }
    }

    describe("EventListener#eventClasses") {
        it("can derive event classes for handlers") {
            class FirstProjector {
                @Suppress("UNUSED_PARAMETER")
                fun project(event: TestEvent, aggregateId: UUID) = Unit
            }
            class SecondProjector {
                @Suppress("UNUSED_PARAMETER")
                fun project(event: AnotherTestEvent, aggregateId: UUID) = Unit
            }

            val eventProcessor = EventProcessor.compose(
                EventProcessor.from(FirstProjector()::project),
                EventProcessor.from(SecondProjector()::project)
            )

            eventProcessor.eventClasses shouldContainExactlyInAnyOrder listOf(FooEvent::class, BarEvent::class, BazEvent::class, QuuxEvent::class)
        }
    }
})

data class SpecificMetadata(val specialField: String) : EventMetadata()

sealed class TestEvent : DomainEvent
data class FooEvent(val foo: String) : TestEvent()
data class BarEvent(val bar: String) : TestEvent()

sealed class AnotherTestEvent : DomainEvent
data class BazEvent(val baz: String) : AnotherTestEvent()
data class QuuxEvent(val quux: String) : AnotherTestEvent()
