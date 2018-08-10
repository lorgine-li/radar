package com.ppdai.infrastructure.ui.controller;

import com.ppdai.infrastructure.radar.biz.dto.ui.UiResponse;
import com.ppdai.infrastructure.ui.dto.request.InstanceSearchRequest;
import com.ppdai.infrastructure.ui.service.UiInstanceService;
import com.ppdai.infrastructure.radar.biz.service.UserService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * InstanceController
 *
 * @author wanghe
 * @date 2018/03/21
 */
@Controller
@RequestMapping("/app/instance")
public class InstanceController {
    @Autowired
    private UiInstanceService uiInstanceService;
    @Autowired
    private UserService userService;

    private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 查询所有instance列表
     *
     * @return
     */
    @RequestMapping("/list")
    public String list() {
        return "app/instance/list";
    }

    /**
     * instance列表数据加载
     *
     * @return
     */
    @RequestMapping("/list/data")
    @ResponseBody
    public UiResponse listData(HttpServletRequest request, InstanceSearchRequest instanceSearchRequest) {
        Map<String, Object> parameterMap = new LinkedHashMap<>();
        parameterMap = setParameterMap(parameterMap, instanceSearchRequest);
        return uiInstanceService.findInstance(request, parameterMap);
    }

    /**
     * 实例详情
     *
     * @param model
     * @param instanceId
     * @return
     */
    @RequestMapping("/detail")
    public String detail(Model model, String instanceId) {
        model.addAttribute("instanceId", instanceId);
        return "app/instance/detail";
    }

    /**
     * 基于instanceId的数据加载
     *
     * @param request
     * @param instanceId
     * @return
     */
    @RequestMapping("/detail/data")
    @ResponseBody
    public UiResponse detailData(HttpServletRequest request, @RequestParam("instanceId") String instanceId) {
        return uiInstanceService.findByInstanceId(request, instanceId);
    }

    /**
     * 展开实例相关的更多信息
     *
     * @param model
     * @param instanceId
     * @return
     */
    @RequestMapping("/expand")
    public String expand(Model model, String instanceId) {
        model.addAttribute("instanceId", instanceId);
        return "app/instance/expand";
    }

    /**
     * 展示我的实例
     * @return
     */
    @RequestMapping("/my")
    public String my(HttpServletRequest request) {
        //如果用户已经登录,则可以查看"我的实例"
        String userId= userService.getCurrentUser().getUserId();
        if (!StringUtils.isEmpty(userId)) {
            return "app/instance/my";
        } else {
            return "app/instance/reminder";
        }
    }

    /**
     * instance列表数据加载
     * @return
     */
    @RequestMapping("/my/data")
    @ResponseBody
    public UiResponse myData(HttpServletRequest request,InstanceSearchRequest instanceSearchRequest) {
        instanceSearchRequest.setDepartmentName(userService.getCurrentUser().getDepartment());
        Map<String, Object> parameterMap = new LinkedHashMap<>();
        parameterMap = setParameterMap(parameterMap, instanceSearchRequest);
        return uiInstanceService.findInstance(request, parameterMap);
    }

    /**
     * 展示过期实例
     *
     * @param request
     * @return
     */
    @RequestMapping("/expired")
    public String expired(HttpServletRequest request) {
        return "app/instance/expired";
    }

    /**
     * 过期列表数据加载
     *
     * @param request
     * @return
     */
    @RequestMapping("/expired/data")
    @ResponseBody
    public UiResponse expiredData(HttpServletRequest request, InstanceSearchRequest instanceSearchRequest) {
        Map<String, Object> parameterMap = new LinkedHashMap<>();
        parameterMap = setParameterMap(parameterMap, instanceSearchRequest);
        //查询实例过期的截止时间
        if (!StringUtils.isEmpty(instanceSearchRequest.getStatusSelect())&&Integer.parseInt(instanceSearchRequest.getStatusSelect())!=0) {
            long lastNoActiveTime = System.currentTimeMillis() - Integer.parseInt(instanceSearchRequest.getStatusSelect()) * 60 * 1000;
            parameterMap.put("lastNoActiveTime", formatter.format(new Date(lastNoActiveTime)));
        }else{
            parameterMap.put("statusSelect",null);
        }
        return uiInstanceService.findExpiredInstance(request, parameterMap);
    }


    /**
     * 修改发布槽位
     *
     * @param request
     * @param pubStatus
     * @param instanceId
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/update/publish/status", method = RequestMethod.POST)
    public String updatePublishStatus(HttpServletRequest request, @RequestParam("pubStatus") String pubStatus, @RequestParam("instanceId") long instanceId) {
        return uiInstanceService.updatePublishStatus(request, pubStatus, instanceId);
    }

    /**
     * 修改超级槽位
     *
     * @param request
     * @param originalStatus
     * @param supperStatus
     * @param instanceId
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/update/supper/status", method = RequestMethod.POST)
    public String updateSupperStatus(HttpServletRequest request, @RequestParam("originalStatus") String originalStatus, @RequestParam("supperStatus") String supperStatus, @RequestParam("instanceId") long instanceId) {
        return uiInstanceService.updateSupperStatus(request, originalStatus, supperStatus, instanceId);
    }


    public Map<String, Object> setParameterMap(Map<String, Object> parameterMap, InstanceSearchRequest instanceSearchRequest) {
        int pageNum = Integer.parseInt(instanceSearchRequest.getPage());
        int pageSize = Integer.parseInt(instanceSearchRequest.getLimit());
        parameterMap.put("statusSelect", instanceSearchRequest.getStatusSelect());
        //instance的自增ID,用于根据id查询
        parameterMap.put("ID", instanceSearchRequest.getId());
        parameterMap.put("clusterName",instanceSearchRequest.getClusterName());
        parameterMap.put("appId", instanceSearchRequest.getAppId());
        parameterMap.put("pageIndex", (pageNum - 1) * pageSize);
        parameterMap.put("pageSize", pageSize);
        //根据部门信息,查询我的实例
        parameterMap.put("departmentName", instanceSearchRequest.getDepartmentName());
        parameterMap.put("appName",instanceSearchRequest.getAppName());
        parameterMap.put("ip",instanceSearchRequest.getIp());
        return parameterMap;
    }


}
