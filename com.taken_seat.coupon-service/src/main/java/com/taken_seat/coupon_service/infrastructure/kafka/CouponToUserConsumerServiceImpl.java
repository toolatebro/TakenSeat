package com.taken_seat.coupon_service.infrastructure.kafka;

import com.taken_seat.common_service.exception.customException.CouponException;
import com.taken_seat.common_service.exception.enums.ResponseCode;
import com.taken_seat.common_service.message.KafkaUserInfoMessage;
import com.taken_seat.coupon_service.application.kafka.CouponToUserConsumerService;
import com.taken_seat.coupon_service.domain.entity.Coupon;
import com.taken_seat.coupon_service.domain.repository.CouponRepository;
import com.taken_seat.coupon_service.infrastructure.config.redis.RedisOperationService;
import io.micrometer.core.annotation.Counted;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class CouponToUserConsumerServiceImpl implements CouponToUserConsumerService {

    private final CouponRepository couponRepository;
    private final RedisOperationService redisOperationService;
    private final RedissonClient redissonClient;
    private final MeterRegistry meterRegistry;

    public CouponToUserConsumerServiceImpl(CouponRepository couponRepository,
                                           RedisOperationService redisOperationService,
                                           RedissonClient redissonClient, MeterRegistry meterRegistry) {
        this.couponRepository = couponRepository;
        this.redisOperationService = redisOperationService;
        this.redissonClient = redissonClient;
        this.meterRegistry = meterRegistry;
    }

    @Counted(value = "coupon.issue.count", description = "쿠폰 발급 횟수 카운트")
    @Transactional
    @Override
    public KafkaUserInfoMessage producerMessage(KafkaUserInfoMessage message) {
        UUID couponId = message.getCouponId();
        UUID userId = message.getUserId();
        String redisKey = "couponId:" + couponId;
        String issuedUserKey = "issuedUsers:" + couponId;
        String lockKey = "lock:coupon:" + couponId;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            if (lock.tryLock(1, 3, TimeUnit.SECONDS)) {
                try {
                    Coupon coupon = couponRepository.findByIdAndDeletedAtIsNull(couponId)
                            .orElseThrow(() -> {
                                recordFailMetric("쿠폰을 찾을 수 없습니다.");
                                return new CouponException(ResponseCode.COUPON_NOT_FOUND);
                            });

                    initializeQuantity(redisKey, coupon);
                    Long currentQuantity = redisOperationService.getCurrentQuantity(redisKey);
                    if (currentQuantity == null || currentQuantity <= 0) {
                        log.warn("쿠폰이 모두 소진되었습니다. couponId: {}, currentQuantity: {}", couponId, currentQuantity);
                        return createFailedMessage(message, "쿠폰이 모두 소진되었습니다.");
                    }

                    // Lua 스크립트 실행
                    Long updatedQuantity = redisOperationService.evalScript(
                            redisKey, issuedUserKey, userId.toString());
                    QuantityUpdatedResponseType updatedResponse = QuantityUpdatedResponseType.valueOf(updatedQuantity)
                            .orElseThrow();

                    switch(updatedResponse) {
                        case NO_QUANTITY:
                            log.warn("쿠폰 수량이 부족합니다. couponId: {}", couponId);
                            recordFailMetric("쿠폰이 모두 소진되었습니다.");
                            throw new CouponException(ResponseCode.COUPON_QUANTITY_EXCEPTION);
                        case ALREADY_APPLIED:
                            log.error("이미 발급에 성공한 유저입니다. userId: {}", userId);
                            recordFailMetric("이미 발급에 성공한 유저입니다.");
                            throw new CouponException(ResponseCode.COUPON_HAS_USER);
                        default:
                            coupon.updateQuantity(updatedQuantity, userId, coupon);
                            coupon.delete(UUID.fromString("00000000-0000-0000-0000-000000000000"));
                            return createSuccessMessage(message, coupon);
                    }
                }finally {
                    lock.unlock();
                }
            } else {
                log.warn("락 획득 실패: couponId: {}", couponId);
                return createFailedMessage(message, "락 획득에 실패했습니다. ");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return createFailedMessage(message, "쓰레드가 중단 되었습니다.");
        }catch (CouponException e) {
            log.warn("쿠폰 관련 예외 발생 : {}", e.getMessage());
            return createFailedMessage(message, e.getErrorCode().getMessage());
        }catch (Exception e) {
            log.error("예상치 못한 예외 발생: ", e);
            return createFailedMessage(message, "서버 에러");
        }
    }

    private void initializeQuantity(String redisKey, Coupon coupon) {
        if (!redisOperationService.hasKey(redisKey)) {
            redisOperationService.initializeQuantity(redisKey, coupon.getQuantity());
        }
    }

    private void recordFailMetric(String reason) {
        meterRegistry.counter("coupon.issue.count", "status", "failed", "reason", reason).increment();
    }

    private KafkaUserInfoMessage createSuccessMessage(KafkaUserInfoMessage message, Coupon coupon) {
        message.success(
                message.getUserId(),
                message.getCouponId(),
                coupon.getDiscount(),
                coupon.getExpiredAt(),
                KafkaUserInfoMessage.Status.SUCCEEDED
        );
        meterRegistry.counter("coupon.issue.count", "status", "succeeded", "reason", "쿠폰 발급에 성공했습니다!").increment();
        log.info("쿠폰 발급 성공: couponId: {}, userId: {}", message.getCouponId(), message.getUserId());
        return message;
    }

    private KafkaUserInfoMessage createFailedMessage(KafkaUserInfoMessage message, String reason) {
        message.failed(
                message.getUserId(),
                message.getCouponId(),
                KafkaUserInfoMessage.Status.FAILED
        );
        meterRegistry.counter("coupon.issue.count", "status", "failed", "reason", reason).increment();
        log.info("쿠폰 발급 실패: couponId: {}, userId: {}, reason: {}", message.getCouponId(), message.getUserId(), reason);
        return message;
    }
}