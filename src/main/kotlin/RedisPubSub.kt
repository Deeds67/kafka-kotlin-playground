
import io.lettuce.core.RedisClient
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands
import io.reactivex.rxjava3.core.Observable
import java.time.Instant



object RedisPubSub {
    val redisClient: RedisClient = RedisClient.create("redis://localhost:6379")
    val connection: StatefulRedisPubSubConnection<String, String> = redisClient.connectPubSub()
    val reactive: RedisPubSubReactiveCommands<String, String> = connection.reactive()
    fun observe(channel: String): Observable<String> {
        return Observable.create { emitter ->
            reactive.subscribe(channel).subscribe()
            emitter.setCancellable { reactive.unsubscribe(channel) }

            reactive.observeChannels().doOnNext {
                // val values: FloatArray = deserializeFloats(it.message)
                val currentTimestamp = Instant.now().epochSecond
                emitter.onNext(it.message)
            }.subscribe()
        }
    }
}