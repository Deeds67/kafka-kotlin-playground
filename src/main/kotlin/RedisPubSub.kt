import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.disposables.Disposable
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPubSub
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

object RedisPubSub {
    fun observe(jedis: Jedis, channel: String, executor: ExecutorService = Executors.newSingleThreadExecutor()): Observable<String> {
        return Observable.create { emitter ->
            val jedisPubSub = object : JedisPubSub() {
                override fun onMessage(channel: String, message: String) {
                    executor.execute { emitter.onNext(message) }
                }

                override fun onUnsubscribe(channel: String, subscribedChannels: Int) {
                    executor.execute { emitter.onComplete() }
                }

                fun onError(channel: String, message: String, exception: Exception) {
                    executor.execute { emitter.onError(exception) }
                }
            }
            executor.execute { jedis.subscribe(jedisPubSub, channel) }

            emitter.setCancellable { jedisPubSub.unsubscribe() }
        }
    }
}

class RedisObserver(private val jedis: Jedis, private val channel: String) : Observer<String> {
    override fun onSubscribe(d: Disposable) {
        // Handle subscription
    }

    override fun onNext(message: String) {
        jedis.publish(channel, message)
    }

    override fun onError(e: Throwable) {
        // Handle error
    }

    override fun onComplete() {
        // Handle completion
    }
}