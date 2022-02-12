package com.mqk.gmall.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class MyKafkaUtil {
	private static final String brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
	private static final String default_topic = "DWD_DEFAULT_TOPIC";

	public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
		return new FlinkKafkaProducer<>(
				brokers,
				topic,
				new SimpleStringSchema());
	}

	public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){
		final Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		return new FlinkKafkaProducer<>(
				default_topic,
				kafkaSerializationSchema,
				properties,
				FlinkKafkaProducer.Semantic.NONE);
	}

	public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId){
		final Properties properties = new Properties();
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		return new FlinkKafkaConsumer<>(
				topic,
				new SimpleStringSchema(),
				properties
		);
	}

}
