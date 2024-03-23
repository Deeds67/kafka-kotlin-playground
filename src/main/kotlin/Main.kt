
import io.lettuce.core.RedisClient
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.disposables.Disposable
import java.util.concurrent.CountDownLatch

fun main() {
    // Create a new RedisClient using the default settings
    val redisClient: RedisClient = RedisClient.create("redis://localhost:6379")

    val channels = listOf("channel", "channel2")

    val observables = channels.map { RedisPubSub.observe(redisClient, it) }

    val mergedObservable: Observable<String> = Observable.merge(observables)

    val disposable = mergedObservable.subscribe(
        { message -> println("Received message: $message") },  // onNext
        { error -> println("Error: ${error.message}") },  // onError
        { println("Completed") }  // onComplete
    )

    // Use a CountDownLatch to keep the application running
    val latch = CountDownLatch(1)

    // Add a shutdown hook to count down the latch when the application is terminated
    Runtime.getRuntime().addShutdownHook(Thread { latch.countDown() })

    // Wait for the latch to count down
    latch.await()

    // Dispose of the disposable when the application is terminated
    disposable.dispose()
}
