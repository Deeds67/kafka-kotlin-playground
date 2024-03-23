
import io.lettuce.core.RedisClient
import io.reactivex.rxjava3.disposables.Disposable
import java.util.concurrent.CountDownLatch

fun main() {
    // Create a new RedisClient using the default settings
    val redisClient: RedisClient = RedisClient.create("redis://localhost:6379")

    // The channel to observe
    val channel1 = "channel"
    val channel2 = "channel2"

    // Call the observe function and subscribe to the Observable it returns
    val disposable: Disposable = RedisPubSub.observe(redisClient, channel1).subscribe(
        { message -> println("1> Received message: $message") },  // onNext
        { error -> println("Error: ${error.message}") },  // onError
        { println("Completed") }  // onComplete
    )

    val disposable2: Disposable = RedisPubSub.observe(redisClient, channel2).subscribe(
        { message -> println("2> Received message: $message") },  // onNext
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
    disposable2.dispose()

}