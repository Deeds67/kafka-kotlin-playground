import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import org.reactivestreams.Subscription
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPubSub
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors


object RedisPubSub {
    @JvmOverloads
    fun observe(
        jedis: Jedis,
        channel: String?,
        executor: ExecutorService = Executors.newSingleThreadExecutor()
    ): Observable<String> {
        return Observable.defer(object : Func0<Observable<String?>?>() {
            fun call(): Observable<String> {
                return Observable.create(RedisObservable(jedis, channel, executor))
            }
        })
    }

    private class RedisObservable(
        private val jedis: Jedis,
        private val channel: String?,
        private val executor: Executor
    ) :
        Func1<Observer<String?>?, Subscription?> {
        fun call(observer: Observer<String?>): Subscription {
            val pubSub: JedisPubSub = object : JedisPubSub() {
                override fun onMessage(channel: String, message: String) {
                    observer.onNext(message)
                }

                override fun onPMessage(
                    pattern: String, channel: String,
                    message: String
                ) {
                }

                override fun onSubscribe(channel: String, subscribedChannels: Int) {
                }

                override fun onUnsubscribe(channel: String, subscribedChannels: Int) {
                }


                override fun onPUnsubscribe(pattern: String, subscribedChannels: Int) {
                }

                override fun onPSubscribe(pattern: String, subscribedChannels: Int) {
                }
            }

            executor.execute { jedis.subscribe(pubSub, channel) }

            return object : Subscription {
                fun unsubscribe() {
                    pubSub.unsubscribe()
                }
            }
        }
    }
}