
import io.lettuce.core.RedisClient
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.schedulers.Schedulers
import java.math.BigDecimal
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
// TODO: Deserialize into a "bid,ask,bidVolume,askVolume,timestamp" data class
// TODO: Use a "ConnectableObservable" for the RedisPubSub observable - so that the same value can be used by other calculations
import java.time.Instant

data class PricePoint(val bid: Double, val ask: Double, val bidVolume: Double, val askVolume: Double,val epochSecond: Long , val tsDiff: Long )

data class BothPricePoints(val pp1: PricePoint, val pp2: PricePoint)



fun deserializePricePoint(input: String): PricePoint {

    val floats = input.split(";")
    return PricePoint( floats[0].toDouble(),  floats[1].toDouble(),  floats[2].toDouble(), floats[3].toDouble(),  floats[4].toLong(), floats[5].toLong())
}



fun calculateAgeInSeconds(timestamp: Long): Long {
    val currentTimestamp = Instant.now().epochSecond
    val timestampInSeconds = (timestamp/1000)
    println("Feed ${timestampInSeconds}    Current ${currentTimestamp} ")
    return (currentTimestamp - timestampInSeconds)
}
fun main() {
    // Create a new RedisClient using the default settings

    val channels = listOf("exch-1-ethusdc",
        "exch-1-ethusdeth-240405",
        "exch-1-ethusdeth-240426",
        "exch-1-ethusdeth-240628",
        "exch-1-ethusdeth-241227")

    // val observables = channels.map { RedisPubSub.observe(redisClient, it).debounce( 100, TimeUnit.MILLISECONDS )}
    val computationScheduler = Schedulers.computation()


//        .debounce( 100, TimeUnit.MILLISECONDS ).
    val observableEthusdc = RedisPubSub.observe("exch-1-ethusdc").subscribeOn(computationScheduler).map {deserializePricePoint(it)}
    val observable240405 = RedisPubSub.observe("exch-1-ethusdeth-240405").subscribeOn(computationScheduler).map {deserializePricePoint(it)}
//    val observable240426 = RedisPubSub.observe(redisClient, "exch-1-ethusdeth-240426").subscribeOn(computationScheduler).debounce( 100, TimeUnit.MILLISECONDS ).map {deserializeFloats(it)}
//    val observable240628 = RedisPubSub.observe(redisClient, "exch-1-ethusdeth-240628").subscribeOn(computationScheduler).debounce( 100, TimeUnit.MILLISECONDS ).map {deserializeFloats(it)}
//    val observable241227 = RedisPubSub.observe(redisClient, "exch-1-ethusdeth-241227").subscribeOn(computationScheduler).debounce( 100, TimeUnit.MILLISECONDS ).map {deserializeFloats(it)}

    // val combinedObservable: Observable<List<String>> = Observable.combineLatest(observables) { it.toList() as List<String> }

    val observableSpot240405 : Observable<Double> = Observable.combineLatest(listOf(observableEthusdc, observable240405)) { inputs ->
        val typedInputs = inputs as Iterable<Observable<PricePoint>>

        val ethusdc = typedInputs.toList()[0] as PricePoint
        val ethusdeth240405 = typedInputs.toList()[1] as PricePoint

        val ethusdc_bid = ethusdc.bid
        val ethusdc_ts = ethusdc.epochSecond
        val ethusdeth240405_ask = ethusdeth240405.ask
        val ethusdeth240405_ts = ethusdeth240405.epochSecond
         println("Timestamp diff: ${ethusdc_ts - ethusdeth240405_ts} Age in seconds ${calculateAgeInSeconds (ethusdc_ts)}  ${calculateAgeInSeconds(ethusdeth240405_ts)}")
        ethusdc_bid - ethusdeth240405_ask
    }
//    val observableSpot240426 : Observable<Float> = Observable.combineLatest(listOf(observableEthusdc, observable240426)) { inputs ->
//        val ethusdc = (inputs[0] as FloatArray)[0]
//        val ethusdeth240426 = (inputs[1] as FloatArray)[0]
//        ethusdeth240426 - ethusdc
//    }
//    val observableSpot240628 : Observable<Float> = Observable.combineLatest(listOf(observableEthusdc, observable240628)) { inputs ->
//        val ethusdc = (inputs[0] as FloatArray)[0]
//        val ethusdeth240628 = (inputs[1] as FloatArray)[0]
//        ethusdeth240628 - ethusdc
//    }
//    val observableSpot241227 : Observable<Float> = Observable.combineLatest(listOf(observableEthusdc, observable241227)) { inputs ->
//        val ethusdc = (inputs[0] as FloatArray)[0]
//        val ethusdeth241227 = (inputs[1] as FloatArray)[0]
//        ethusdeth241227 - ethusdc
//    }
//    observableSpot240426, observableSpot240628, observableSpot241227
    val spreadMatrix : Observable<List<Number>> = Observable.combineLatest(listOf(observableSpot240405, )) { inputs ->
        val spreadObservableSpot240405 = inputs[0] as Float
//        val spreadObservableSpot240426 = inputs[1] as Float
//        val spreadObservableSpot240628 = inputs[2] as Float
//        val spreadObservableSpot241227 = inputs[3] as Float
//        spreadObservableSpot240426, spreadObservableSpot240628, spreadObservableSpot241227
        listOf(spreadObservableSpot240405, )
    }

    val disposable = spreadMatrix.subscribeOn(computationScheduler).subscribe(
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
