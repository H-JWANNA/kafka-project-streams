package com.example.kafkastreams.listener;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.example.kafkastreams.utils.JsonUtils;

@Component
public class StreamListener {

	@Bean
	public KStream<String, String> kStream(StreamsBuilder builder) {
		final String inputTopic = "checkout.complete.v1";
		final String outputTopic = "checkout.productId.aggregated.v1";

		KStream<String, String> inputStream = builder.stream(inputTopic);
		inputStream
			.map((k, v) -> new KeyValue<>(JsonUtils.getProductId(v), JsonUtils.getAmount(v)))
			// Group by productId
			.groupByKey(Grouped.with(Serdes.Long(), Serdes.Long()))
			// Window 설정 (event 받는 시점 기준으로 1분 동안의 데이터 그룹핑)
			.windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
			// Apply sum method (위에서 그룹핑 된 Amount 합)
			.reduce(Long::sum)
			// map the window key (다시 outputTopic 으로 내보내기 위해 Stream 생성)
			.toStream((k, v) -> k.key())
			// outputTopic 에 보낼 Json String 으로 Generate
			.mapValues(JsonUtils::getSendingJson)
			// outputTopic 에 보낼 key 값 : null 설정
			.selectKey((k, v) -> null)
			// outputTopic 으로 메시지 key-value (null, jsonString) 전송 설정
			.to(outputTopic, Produced.with(null, Serdes.String()));

		return inputStream;
	}
}
