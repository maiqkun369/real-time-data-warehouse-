package com.mqk.gmall.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

	private static ThreadPoolExecutor threadPoolExecutor = null;

	private ThreadPoolUtil() {
	}

	public static ThreadPoolExecutor getThreadPool() {
		if(null == threadPoolExecutor){
			synchronized (ThreadPoolUtil.class){
				if(null == threadPoolExecutor){
					threadPoolExecutor = new ThreadPoolExecutor(
							8,
							16,
							1,
							TimeUnit.MINUTES,
							new LinkedBlockingDeque<>());
				}
			}
		}
		return threadPoolExecutor;
	}
}
