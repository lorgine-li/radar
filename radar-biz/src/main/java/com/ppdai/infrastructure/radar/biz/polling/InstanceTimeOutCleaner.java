package com.ppdai.infrastructure.radar.biz.polling;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.ppdai.infrastructure.radar.biz.common.SoaConfig;
import com.ppdai.infrastructure.radar.biz.common.thread.SoaThreadFactory;
import com.ppdai.infrastructure.radar.biz.common.util.EmailUtil;
import com.ppdai.infrastructure.radar.biz.common.util.JsonUtil;
import com.ppdai.infrastructure.radar.biz.common.util.Util;
import com.ppdai.infrastructure.radar.biz.entity.AppEntity;
import com.ppdai.infrastructure.radar.biz.entity.InstanceEntity;
import com.ppdai.infrastructure.radar.biz.service.AppService;
import com.ppdai.infrastructure.radar.biz.service.InstanceService;
import com.ppdai.infrastructure.radar.biz.service.SoaLockService;
import com.ppdai.infrastructure.radar.biz.service.TaskService;

@Component
public class InstanceTimeOutCleaner {
	private Logger log = LoggerFactory.getLogger(InstanceTimeOutCleaner.class);
	private ThreadPoolExecutor executor = new ThreadPoolExecutor(3, 3, 0L, TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue<Runnable>(10), SoaThreadFactory.create("InstanceTimeOutCleaner", true),
			new ThreadPoolExecutor.DiscardOldestPolicy());

	private volatile boolean isRunning = false;
	private volatile boolean isMaster = false;
	@Autowired
	private InstanceService instanceService;
	// @Autowired
	private SoaLockService soaLockService = new SoaLockService("soa_clean_sk");
	@Autowired
	private SoaConfig soaConfig;
	@Autowired
	private TaskService taskService;
	@Autowired
	private AppService appService;
	@Autowired
	private Util util;

	@Autowired
	private EmailUtil emailUtil;

	private Date lastDate = new Date();
	private static Object lockObj = new Object();

	public void start() {
		if (!isRunning) {
			synchronized (lockObj) {
				isRunning = true;
				executor.execute(new Runnable() {
					@Override
					public void run() {
						doCheckHearttime();
					}
				});
				executor.execute(new Runnable() {
					@Override
					public void run() {
						clearOldTask();
					}
				});
				executor.execute(new Runnable() {
					@Override
					public void run() {
						clearOldInstance();
					}
				});
			}
		}
	}

	private void clearOldInstance() {
		while (isRunning) {
			try {
				if (isMaster) {
					// 删除过期的Instance数据
					clearOldInstanceData();
				}
			} catch (Exception e) {
				log.error("clearOldTaskError", e);
			}
			Util.sleep(soaConfig.getSoaLockHeartBeatTime() * 1000);
		}

	}

	private void clearOldTask() {
		while (isRunning) {
			try {
				if (isMaster) {
					// 删除过期的task数据
					clearOldTaskData();
				}
			} catch (Exception e) {
				log.error("clearOldTaskError", e);
			}
			Util.sleep(soaConfig.getSoaLockHeartBeatTime() * 1000);
		}

	}

	protected void doCheckHearttime() {
		while (isRunning) {
			try {
				// clearOldTaskData();
				if (soaLockService.isMaster()) {
					isMaster = true;
					// 检查心跳过期的，但是心跳状态不匹配的
					checkExpiredHeartTime();
					// 检查正常的，但是心跳状态不匹配的
					checkNormalHeartTime();
				} else {
					isMaster = false;
				}
			} catch (Exception e) {
				e.printStackTrace();
				log.error("InstanceTimeOutCleanerfail", e);
			}

			Util.sleep(soaConfig.getInstanceCleanInterval());

		}
	}

	private void clearOldTaskData() {
		Date now = new Date();
		// 定时清除数据
		if (now.getTime() - lastDate.getTime() > 1000 * 60 * soaConfig.getTaskCleanInterval()) {
			lastDate = now;
			long minId = taskService.getMinId();
			if (minId > 0) {
				log.info("clear_old_data_minId_is_{}_and_maxId_is_{}", minId, minId + 500);
				int count = taskService.clearOld(60 * soaConfig.getTaskCleanInterval(), minId + 500);
				while (count > 0) {
					minId = taskService.getMinId();
					log.info("clear_old_data_minId_is_{}_and_maxId_is_{}", minId, minId + 500);
					count = taskService.clearOld(60 * soaConfig.getTaskCleanInterval(), minId + 500);
					Util.sleep(300);
				}
			}
		}
	}

	private void clearOldInstanceData() {
		List<InstanceEntity> expireLst = instanceService.findOld(soaConfig.getInstanceClearTime());
		if (expireLst.size() == 0) {
			return;
		}
		String dbNow = Util.formateDate(util.getDbNow());
		expireLst.forEach(t1 -> {
			String content = String.format("心跳时间超过过期时间，被删除,json为:%s,and DbTime is %s", JsonUtil.toJsonNull(t1), dbNow);
			Util.log(log, t1, "timeout_delete_old", content);
			emailUtil.sendWarnMail(
					"clearOldInstance,appId:" + t1.getCandAppId() + ",ip:" + t1.getIp() + "长时间为发送心跳，即将删除", content,
					getMail(t1.getCandAppId()));
		});
		if (expireLst.size() < 20) {
			instanceService.deleteInstance(expireLst);
		} else {
			List<InstanceEntity> instanceEntities = new ArrayList<>(20);
			for (int i = 0; i < expireLst.size(); i++) {
				instanceEntities.add(expireLst.get(i));
				if ((i + 1) % 20 == 0) {
					try {
						instanceService.deleteInstance(instanceEntities);
					} catch (Exception e) {
						e.printStackTrace();
					}
					instanceEntities.clear();
				}
			}
			if (instanceEntities.size() > 0) {
				instanceService.deleteInstance(instanceEntities);
			}
		}

	}

	protected void checkNormalHeartTime() {
		List<InstanceEntity> normalEntity = instanceService.findNoraml(soaConfig.getExpiredTime());
		if (!CollectionUtils.isEmpty(normalEntity)) {
			String dbNow = Util.formateDate(util.getDbNow());
			normalEntity.forEach(t1 -> {
				Util.log(log, t1, "heatBeatNormal",
						String.format("心跳正常，心跳状态变更为1,json为:%s,and DbTime is %s", JsonUtil.toJsonNull(t1), dbNow));
			});
			if (soaLockService.isMaster()) {
				log.info("NormalHeartTime开始更新");
				List<List<InstanceEntity>> rs = Util.split(normalEntity, 50);
				rs.forEach(t1 -> {
					instanceService.updateHeartStatus(t1, true, soaConfig.getExpiredTime());
				});
			}
		}
	}

	private String getMail(String canAppId) {
		if (StringUtils.isEmpty(canAppId)) {
			return "";
		}
		Map<String, AppEntity> appCache = appService.getCacheData();
		if (appCache.containsKey(canAppId)) {
			if (appCache.get(canAppId).getAlarm() == 1) {
				return appCache.get(canAppId).getOwnerEmail();
			}
		}
		return "";
	}

	protected void checkExpiredHeartTime() {
		List<InstanceEntity> expireEntity = instanceService.findExpired(soaConfig.getExpiredTime());
		if (!CollectionUtils.isEmpty(expireEntity)) {
			String dbNow = Util.formateDate(util.getDbNow());
			expireEntity.forEach(t1 -> {
				String content = String.format("超时，心跳状态变更为0,json为:%s,and DbTime is %s", JsonUtil.toJsonNull(t1), dbNow);
				Util.log(log, t1, "heatBeatTimeOut", content);
				if (t1.getInstanceStatus() == 1) {
					emailUtil.sendWarnMail(
							"checkHeartTime,appId:" + t1.getCandAppId() + ",ip:" + t1.getIp() + "心跳超时，服务可能不可用", content,
							getMail(t1.getCandAppId()));
				}
			});
			if (soaLockService.isMaster()) {
				List<List<InstanceEntity>> rs = Util.split(expireEntity, 50);
				log.info("HeartTime开始更新");
				rs.forEach(t1 -> {
					instanceService.updateHeartStatus(t1, false, soaConfig.getExpiredTime());
				});

			}
		}
	}

	public void stop() {
		isRunning = false;
	}
}
