package com.ppdai.infrastructure.rest.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.ppdai.infrastructure.radar.biz.common.trace.MetricSingleton;
import com.ppdai.infrastructure.radar.biz.common.trace.TraceFactory;
import com.ppdai.infrastructure.radar.biz.common.util.IPUtil;
import com.ppdai.infrastructure.radar.biz.dto.base.InstanceDto;
import com.ppdai.infrastructure.radar.biz.dto.client.GetAppRequest;
import com.ppdai.infrastructure.radar.biz.dto.client.GetAppResponse;
import com.ppdai.infrastructure.radar.biz.service.AppCacheService;
import com.ppdai.infrastructure.radar.biz.service.AppClusterService;
import com.ppdai.infrastructure.radar.biz.service.AppService;
import com.ppdai.infrastructure.radar.biz.service.InstanceService;

@RestController
public class StatController {
	private static final Logger log = LoggerFactory.getLogger(StatController.class);
	@Autowired
	private AppService appService;
	@Autowired
	private AppCacheService appCacheService;
	@Autowired
	private InstanceService instanceService;

	@Autowired
	private AppClusterService appClusterService;

	@GetMapping("/app/stat")
	public String stat() {
		return "App.Count is:" + appService.getCount() + "," + "App.ClusterCount is:"
				+ appClusterService.getClusterCount() + ",App.InstanceCount is:" + instanceService.getCount();
	}

	@GetMapping("/app/cache1")
	public Object getCache() {
		return appCacheService.getApp();
	}

	@GetMapping("/app/cache")
	public Object getData(@RequestParam(value = "flag", required = false) Integer flag,
			@RequestParam(value = "appId", required = false) String appId) {
		if (flag == null) {
			flag = 0;
		}
		// servCacheService.initServ();
		GetAppRequest request = new GetAppRequest();
		if (!StringUtils.isEmpty(appId)) {
			Map<String, Long> map = new HashMap<>();
			map.put(appId.toLowerCase(), (long) 0);
			request.setAppVersion(map);
		}
		request.setContainOffline(flag == 0 || flag == 2);
		GetAppResponse response = appService.getApp(request);
		Map<String, Object> mapRs = new HashMap<>();
		final int[] count = new int[2];
		count[0] = 0;
		count[1] = 0;
		final int flag1 = flag;
		List<String> clearName = new ArrayList<>();
		if (response != null) {
			boolean[] boolArr = new boolean[1];
			boolArr[0] = true;
			response.getApp().values().forEach(t1 -> {
				boolArr[0] = true;
				if (!CollectionUtils.isEmpty(t1.getClusters())) {
					t1.getClusters().forEach(t2 -> {
						if (!CollectionUtils.isEmpty(t2.getInstances())) {
							List<InstanceDto> rs = new ArrayList<>(t2.getInstances().size());
							for (InstanceDto instanceDto : t2.getInstances()) {
								if (instanceDto.isStatus()) {
									count[0] = count[0] + 1;
								} else {
									if (flag1 == 2) {
										rs.add(instanceDto);
									}
									count[1] = count[1] + 1;
								}
							}
							if (flag1 == 2) {
								t2.setInstances(rs);
							}
						}
						if (!CollectionUtils.isEmpty(t2.getInstances())) {
							boolArr[0] = false;
						}
					});
				}
				if (boolArr[0]) {
					clearName.add(t1.getAppMeta().getName());
				}
			});
		}
		if (flag == 1) {
			clearName.forEach(t3 -> response.getApp().remove(t3));
		}
		mapRs.put("统计是", "有[" + count[0] + "]个在线实例,有[" + count[1] + "]个下线实例,总共[" + (count[0] + count[1]) + "]个实例!");
		mapRs.put("结果是", response);
		return mapRs;
	}

	@GetMapping("/app/trace")
	public Object getTrace() {
		return TraceFactory.getTraces();
	}

	@GetMapping("/ip")
	public Object getIp() {
		return IPUtil.getLocalIP();
	}
	
	@PostConstruct
	public void report() {
		MetricSingleton.getMetricRegistry().register(MetricRegistry.name("app.Count"), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return appService.getCount();
			}
		});
		MetricSingleton.getMetricRegistry().register(MetricRegistry.name("app.ClusterCount"), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return appClusterService.getClusterCount();
			}
		});
		MetricSingleton.getMetricRegistry().register(MetricRegistry.name("app.InstanceCount"), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return instanceService.getCount();
			}
		});
	}
}
