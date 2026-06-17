package com.taken_seat.performance_service.recommend.application.service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.taken_seat.performance_service.performance.domain.facade.PerformanceFacade;
import com.taken_seat.performance_service.performance.presentation.dto.request.RecommendedPerformanceDto;
import com.taken_seat.performance_service.recommend.infrastructure.redis.RecommendationCacheService;
import com.taken_seat.performance_service.recommend.presentation.dto.response.RecommendedPerformanceResponseDto;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class RecommendQueryService {

	private static final int DEFAULT_TOP_N = 10;

	private final RecommendationCacheService cacheService;
	private final RecommendMatrixService matrixService;
	private final PerformanceFacade performanceFacade;

	/**
	 * 사용자별 추천 퍼포먼스 응답 DTO 목록 반환
	 * 1) Redis 캐시 조회
	 * 2) PENDING 설정 (중복 계산 방지)
	 * 3) 순수 추천 계산 (Cosine 유사도)
	 * 4) 공연 조회 → 요청 DTO 획득
	 * 5) 요청 DTO → 응답 DTO 변환
	 * 6) 추천 결과 Redis 저장
	 */
	@Transactional(readOnly = true)
	public List<RecommendedPerformanceResponseDto> recommendFor(UUID userId) {
		log.info("[Recommend] 요청 시작 - userId={}", userId);

		Optional<List<UUID>> cachedIds = cacheService.getCachedRecommendations(userId);

		if (cachedIds.isPresent()) {
			log.info("[Recommend] 캐시 히트 - userId={}, ids={}", userId, cachedIds.get());
			return getRecommendFromCache(cachedIds.get(), userId);
		}
		log.info("[Recommend] 캐시 미스 - userId={}", userId);

		boolean pendingSet = cacheService.setPendingIfAbsent(userId);
		log.info("[Recommend] PENDING 설정 시도 - userId={}, success={}", userId, pendingSet);
		if (!pendingSet) {
			log.info("[Recommend] 이미 계산 중 - userId={}", userId);
			return List.of();
		}

		List<UUID> performanceIds = matrixService.recommendFor(userId, DEFAULT_TOP_N);
		log.info("[Recommend] 계산 완료 - userId={}, recommendedIds={}", userId, performanceIds);

		List<RecommendedPerformanceDto> requestDtos = performanceFacade.findRecommendedPerformancesByIds(
			performanceIds);
		log.info("[Recommend] 요청 DTO 획득 완료 - count={}", requestDtos.size());

		List<RecommendedPerformanceResponseDto> responses = requestDtos.stream()
			.map(req -> new RecommendedPerformanceResponseDto(
				req.performanceId(),
				req.title(),
				req.StartAt()
			))
			.toList();
		log.info("[Recommend] 응답 DTO 변환 완료 - count={}", responses.size());

		cacheService.saveRecommendations(userId, performanceIds);
		log.info("[Recommend] 캐시 저장 완료 - userId={}", userId);

		return responses;
	}

	// 캐시로 불러오는 로직을 분리
	private List<RecommendedPerformanceResponseDto> getRecommendFromCache(List<UUID> cachedIds, UUID userId) {
		List<RecommendedPerformanceResponseDto> responses = performanceFacade
				.findRecommendedPerformancesByIds(cachedIds)
				.stream()
				.map(req -> new RecommendedPerformanceResponseDto(
						req.performanceId(),
						req.title(),
						req.StartAt()
				))
				.toList();
		log.info("[Recommend] 캐시 반환 완료 - userId={}, count={}", userId, responses.size());
		return responses;
	}
}
