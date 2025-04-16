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
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.floor

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        // MODIFICATION: Добавляем множество HTTP-кодов, для которых потребуется повторная попытка.
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
        .readTimeout(70, TimeUnit.SECONDS)
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        concurrencyLimiter.acquire()
        val transactionId = UUID.randomUUID()
        try {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            if (isOverDeadline(deadline)) {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request deadline.")
                }
                return
            }

            while (!tokenBucket.tick()) {
                Thread.sleep(5)
            }

            logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")
            // Фиксируем факт отправки платежа (независимо от результата)
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            if (isOverDeadline(deadline)) {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request deadline.")
                }
                return
            }

            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            // MODIFICATION: Вместо прямого вызова HTTP-запроса вызываем рекурсивный метод,
            // реализующий синхронное выполнение с повторными попытками.
            processPaymentReqSync(request, transactionId, paymentId, paymentStartedAt, deadline)

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

    // MODIFICATION: Рекурсивный метод для синхронной обработки запроса с retry‑логикой.
    private fun processPaymentReqSync(
        request: Request,
        transactionId: UUID,
        paymentId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        attemptNum: Int = 0
    ) {
        // Увеличиваем счетчик попыток; поскольку attemptNum не меняется, создаем новую переменную.
        val nextAttempt = attemptNum + 1

        // Ограничиваем число попыток, рассчитывая максимум, допустимый исходя из оставшегося времени.
        if (nextAttempt > floor((deadline - paymentStartedAt - 1).toDouble() / properties.averageProcessingTime.toMillis() - 1).toLong()) {
            logger.warn("[$accountName] Exceeded maximum attempts ($nextAttempt) for txId: $transactionId, payment: $paymentId")
            return
        }

        client.newCall(request).execute().use { response ->
            val body = try {
                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            // Обновляем статус платежа — вне зависимости от результата.
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }

            // Если запрос не успешен или получен код, требующий повторной попытки, и дедлайн еще не закрыт,
            // ждём согласно rateLimiter.tickBlocking() и повторяем вызов.
            if ((!body.result || RETRYABLE_HTTP_CODES.contains(response.code)) && !isOverDeadline(deadline)) {
                tokenBucket.tick()
                processPaymentReqSync(request, transactionId, paymentId, paymentStartedAt, deadline, nextAttempt)
            }
        }
    }

    // MODIFICATION: Новый метод проверки оставшегося времени (дедлайна).
    // Считаем, что если до дедлайна осталось меньше, чем дважды среднее время обработки, то начинать новую попытку не стоит.
    private fun isOverDeadline(deadline: Long): Boolean {
        return deadline - now() < properties.averageProcessingTime.toMillis() * 2
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
