package com.taken_seat.performance_service.recommend.application.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.stereotype.Service;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class RecommendMatrixService {

	private final Map<UUID, Set<UUID>> userPerformanceMap = new Object2ObjectOpenHashMap<>();

	public void registerBooking(UUID userId, UUID performanceId, int ticketCount) {

		userPerformanceMap
			.computeIfAbsent(userId, key -> new HashSet<>())
			.add(performanceId);
	}

	public List<UUID> recommendFor(UUID targetUserId, int topN) {

		Set<UUID> targetVector = userPerformanceMap.get(targetUserId);

		if (targetVector == null || targetVector.isEmpty()) {
			// 예매 데이터가 없다는 것은 일반적이기 때문에 log level 을 info 로 변경합니다.
			log.info("[Recommend] 추천 불가 - 예매 데이터 없음 - userId={}", targetUserId);

			// 추천이 불가 할 경우 기본 정렬로 리턴하는 것이 좋습니다. (UI 영역을 비을수 없기 때문에)
			return getDefaultRecommendItems(topN);
		}

		Map<UUID, Double> similarityMap = new HashMap<>();

		for (Map.Entry<UUID, Set<UUID>> entry : userPerformanceMap.entrySet()) {
			UUID otherUserId = entry.getKey();
			if (otherUserId.equals(targetUserId))
				continue;

			Set<UUID> otherVector = entry.getValue();
			double similarity =
				cosineSimilarity(targetVector, otherVector);

			if (similarity > 0.0) {
				similarityMap.put(otherUserId, similarity);
				log.info("user={} 와 user={} 의 유사도 = {}", targetUserId, otherUserId, similarity);
			}
		}

		Map<UUID, Double> candidateScores = new HashMap<>();

		for (Map.Entry<UUID, Double> similarityEntry : similarityMap.entrySet()) {
			UUID similarUserId = similarityEntry.getKey();
			double score = similarityEntry.getValue();
			Set<UUID> performances
				= userPerformanceMap.get(similarUserId);
			for (UUID performanceId : performances) {
				if (!targetVector.contains(performanceId)) {
					candidateScores.put(performanceId, candidateScores.getOrDefault(
						performanceId, 0.0) + score
					);
				}
			}
		}

		return candidateScores.entrySet()
			.stream()
			.sorted(Map.Entry.<UUID, Double>comparingByValue().reversed())
			.limit(topN)
			.map(Map.Entry::getKey)
			.toList();
	}

	// 기본 정렬
	private List<UUID> getDefaultRecommendItems(int topN) {
		Map<UUID, Integer> frequencyMap = new HashMap<>();

		for (Set<UUID> valueSet : userPerformanceMap.values()) {
			for (UUID uuid : valueSet) {
				frequencyMap.put(uuid, frequencyMap.getOrDefault(uuid, 0) + 1);
			}
		}

		return frequencyMap.entrySet()
				.stream()
				.sorted((e1, e2) -> Integer.compare(e2.getValue(), e1.getValue())) // 내림차순 정렬
				.limit(topN)
				.map(Map.Entry::getKey)
				.toList();
	}

	private double cosineSimilarity(Set<UUID> userVectorA, Set<UUID> userVectorB) {

		if (userVectorA.isEmpty() || userVectorB.isEmpty()) {
			log.info("유사도 계산 실패: 한쪽 벡터가 비어있음");
			return 0.0;
		}

		Set<UUID> commonPerformances = new HashSet<>(userVectorA);
		commonPerformances.retainAll(userVectorB);

		double dotProduct = commonPerformances.size();
		double magnitudeOfA = Math.sqrt(userVectorA.size());
		double magnitudeOfB = Math.sqrt(userVectorB.size());

		double similarity = dotProduct / (magnitudeOfA * magnitudeOfB);

		log.info("cosineSimilarity: 공통 공연 개수 = {}, A크기 = {}, B크기 = {}, 유사도 = {}",
			dotProduct, userVectorA.size(), userVectorB.size(), similarity);

		return similarity;
	}
}
