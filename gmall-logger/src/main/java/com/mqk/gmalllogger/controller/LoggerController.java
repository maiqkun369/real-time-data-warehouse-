package com.mqk.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@SuppressWarnings("all")
public class LoggerController {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@GetMapping("applog")
	public String getLog(@RequestParam("param")String jsonStr){

		//数据落盘, 以日志的方式写入磁盘，logback方式
		log.info(jsonStr);

		//数据写入kafka
		kafkaTemplate.send("ods_base_log", jsonStr);


		return "success";
	}


}
