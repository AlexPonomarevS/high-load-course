package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.LinkedList
import java.util.concurrent.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.floor

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        val RETRYABLE_HTTP_CODES = setOf(429, 500, 502, 503, 504)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val tokenBucket = TokenBucketRateLimiter(
        rate = rateLimitPerSec,
        bucketMaxCapacity = rateLimitPerSec,
        window = 1,
        timeUnit = TimeUnit.SECONDS
    )
    private val concurrencyLimiter = Semaphore(parallelRequests)
    private val client = OkHttpClient.Builder()
        .callTimeout(Duration.ofMillis(requestAverageProcessingTime.toMillis() + 100))
        .build()

    private val processingTimes = LinkedList<Long>()
    private val processingTimesMaxSize = rateLimitPerSec * 2
    private val mutex = ReentrantLock()

    private val boundedQueue = LinkedBlockingQueue<Runnable>(12)
    private val pool = ThreadPoolExecutor(
        12, 32,
        70, TimeUnit.SECONDS,
        boundedQueue,
        Executors.defaultThreadFactory(),
        ThreadPoolExecutor.AbortPolicy()
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId, amount: $amount")
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}" +
                    "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=${Duration.ofMillis(requestAverageProcessingTime.toMillis() - 50)}")
            post(emptyBody)
        }.build()
        if (isOverDeadline(deadline)) {
            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request deadline.")
            }
            return
        }
        try {
            pool.submit {
                try {
                    concurrencyLimiter.acquire()
                    if (isOverDeadline(deadline)) {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId")
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request deadline.")
                        }
                        return@submit
                    }
                    tokenBucket.tick()
                    if (isOverDeadline(deadline)) {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId")
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request deadline.")
                        }
                        return@submit
                    }
                    processPaymentReqSync(request, transactionId, paymentId, amount, paymentStartedAt, deadline)
                } catch (e: Exception) {
                    when (e) {
                        is SocketTimeoutException -> {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }
                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                } finally {
                    concurrencyLimiter.release()
                }
            }
        } catch (e: RejectedExecutionException) {
            if (isOverDeadline(deadline)) {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request deadline.")
                }
                return
            }
            performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    private fun processPaymentReqSync(
        request: Request,
        transactionId: UUID,
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attemptNum: Int = 0
    ) {
        if (!isNextAttemptRational(attemptNum, amount)) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request's next attempt isn't rational.")
            }
            return
        }
        val nextAttempt = attemptNum + 1
        val currentAvgPT = avgPT()
        if (isOverDeadline(deadline, currentAvgPT)) {
            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId - deadline imminent (avgPT: ${currentAvgPT.toMillis()} ms)")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request deadline.")
            }
            return
        }
        val startTime = now()
        try {
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}", e)
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
                addProcessingTime(now() - startTime)
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
                if ((!body.result || RETRYABLE_HTTP_CODES.contains(response.code)) && !isOverDeadline(deadline)) {
                    waitUntilNextAllowedAttempt(startTime)
                    processPaymentReqSync(request, transactionId, paymentId, amount, paymentStartedAt, deadline, nextAttempt)
                }
            }
        } catch (e: Exception) {
            if (!isOverDeadline(deadline, currentAvgPT)) {
                waitUntilNextAllowedAttempt(startTime)
                processPaymentReqSync(request, transactionId, paymentId, amount, paymentStartedAt, deadline, nextAttempt)
            } else {
                throw e
            }
        }
    }

    private fun waitUntilNextAllowedAttempt(startTime: Long) {
        tokenBucket.tick()
        val delta = now() - startTime
        if (delta < 1000) {
            Thread.sleep(1000 - delta)
        }
    }

    private fun isNextAttemptRational(attemptNum: Int, amount: Int): Boolean {
        if (properties.price >= amount) return false
        if (attemptNum >= floor(6000.0 / properties.averageProcessingTime.toMillis())) return false
        val totalCostIfFail = (attemptNum + 1) * properties.price
        val expectedProfit = amount - totalCostIfFail
        return expectedProfit >= 0
    }

    private fun addProcessingTime(duration: Long) {
        mutex.withLock {
            processingTimes.add(duration)
            while (processingTimes.size > processingTimesMaxSize) {
                processingTimes.removeFirst()
            }
        }
    }

    private fun calcPT(quantile: Double): Long {
        mutex.withLock {
            if (processingTimes.size < processingTimesMaxSize) {
                return properties.averageProcessingTime.toMillis()
            }
            val sorted = processingTimes.sorted()
            val size = sorted.size
            val quantileIndex = (size - 1) * quantile
            val lowerIndex = quantileIndex.toInt()
            val fraction = quantileIndex - lowerIndex
            return if (lowerIndex >= size - 1) {
                sorted.last()
            } else {
                (sorted[lowerIndex] + (sorted[lowerIndex + 1] - sorted[lowerIndex]) * fraction).toLong()
            }
        }
    }

    private fun avgPT(): Duration {
        val quantileValue = calcPT(0.5)
        return Duration.ofMillis(quantileValue)
    }

    private fun isOverDeadline(deadline: Long, avgPT: Duration): Boolean {
        return deadline - now() < avgPT.toMillis()
    }

    private fun isOverDeadline(deadline: Long): Boolean {
        return deadline - now() < properties.averageProcessingTime.toMillis() * 2
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
