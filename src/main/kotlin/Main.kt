
import redis.clients.jedis.JedisPool


fun main(args: Array<String>) {

    val pool = JedisPool("localhost", 6379)
    pool.resource.use { jedis ->
        jedis.set("clientName", "Jedis")
    }
    println("done")

}