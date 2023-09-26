package com.cultureamp.eventsourcing

import arrow.core.Either
import arrow.core.left

interface RetryStrategy {
    fun tryRunning(fn: () -> Either<CommandError, SuccessStatus>): Either<CommandError, SuccessStatus>
}

// this mimics the default retry behaviour from earlier versions of Kestrel
class FixedTriesThreadSleep(
    private val retries: Int = 5,
    private val delayMs: Long = 500L
) : RetryStrategy {
    override fun tryRunning(fn: () -> Either<CommandError, SuccessStatus>): Either<CommandError, SuccessStatus> {
        for (i in retries downTo 0) {
            val result = fn()
            if (result.isLeft { it is RetriableError } && i > 0) {
                Thread.sleep(delayMs)
            } else {
                return result
            }
        }
        throw RuntimeException("Managed to reach unreachable code")
    }
}


interface CommandGateway<M : EventMetadata> {
    companion object {
        operator fun <M : EventMetadata> invoke(
            eventStore: EventStore<M>,
            vararg routes: Route<*, *, M>
        ) = EventStoreCommandGateway(eventStore, *routes)
    }

    fun dispatch(
        command: Command,
        metadata: M,
        retryStrategy: RetryStrategy = FixedTriesThreadSleep()
    ): Either<CommandError, SuccessStatus>
}


class EventStoreCommandGateway<M : EventMetadata>(
    private val eventStore: EventStore<M>,
    private vararg val routes: Route<*, *, M>
) : CommandGateway<M> {
    override fun dispatch(
        command: Command,
        metadata: M,
        retryStrategy: RetryStrategy
    ): Either<CommandError, SuccessStatus> =
        retryStrategy.tryRunning { createOrUpdate(command, metadata) }

    private fun createOrUpdate(command: Command, metadata: M): Either<CommandError, SuccessStatus> {
        val constructor = constructorFor(command) ?: return NoConstructorForCommand.left()
        val events = eventStore.eventsFor(command.aggregateId)
        return if (events.isEmpty()) when (command) {
            is CreationCommand -> constructor.create(command, metadata, eventStore).map { Created }
            else -> AggregateNotFound.left()
        } else when (command) {
            is UpdateCommand -> constructor.update(command, metadata, events, eventStore)
                .map { Updated }

            else -> AggregateAlreadyExists.left()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun constructorFor(command: Command): AggregateConstructor<CreationCommand, CreationEvent, DomainError, UpdateCommand, UpdateEvent, M, Aggregate<UpdateCommand, UpdateEvent, DomainError, M, *>>? {
        val route = routes.find {
            it.creationCommandClass.isInstance(command) || it.updateCommandClass.isInstance(command)
        }
        return route?.aggregateConstructor as AggregateConstructor<CreationCommand, CreationEvent, DomainError, UpdateCommand, UpdateEvent, M, Aggregate<UpdateCommand, UpdateEvent, DomainError, M, *>>?
    }
}

sealed class SuccessStatus
object Created : SuccessStatus()
object Updated : SuccessStatus()

object NoConstructorForCommand : CommandError
object AggregateAlreadyExists : CommandError
object AggregateNotFound : CommandError
