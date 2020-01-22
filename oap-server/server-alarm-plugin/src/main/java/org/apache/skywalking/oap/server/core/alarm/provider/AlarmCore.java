/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.core.alarm.provider;

import java.util.*;
import java.util.concurrent.*;
import org.apache.skywalking.oap.server.core.alarm.*;
import org.joda.time.*;
import org.slf4j.*;

/**
 * Alarm core includes metrics values in certain time windows based on alarm settings. By using its internal timer
 * trigger and the alarm rules to decides whether send the alarm to database and webhook(s)
 *
 * @author wusheng
 */
public class AlarmCore {
    private static final Logger logger = LoggerFactory.getLogger(AlarmCore.class);

    private Map<String, List<RunningRule>> runningContext;
    private LocalDateTime lastExecuteTime;

    AlarmCore(Rules rules) {
        runningContext = new HashMap<>();
        rules.getRules().forEach(rule -> {
            RunningRule runningRule = new RunningRule(rule);

            String metricsName = rule.getMetricsName();

            List<RunningRule> runningRules = runningContext.computeIfAbsent(metricsName, key -> new ArrayList<>());

            runningRules.add(runningRule);
        });
    }

    public List<RunningRule> findRunningRule(String metricsName) {
        return runningContext.get(metricsName);
    }

    public void start(List<AlarmCallback> allCallbacks) {
        LocalDateTime now = LocalDateTime.now();
        lastExecuteTime = now;
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                List<AlarmMessage> alarmMessageList = new ArrayList<>(30);
                LocalDateTime checkTime = LocalDateTime.now();
                int minutes = Minutes.minutesBetween(lastExecuteTime, checkTime).getMinutes();
                boolean[] hasExecute = new boolean[] {false};
                runningContext.values().forEach(ruleList -> ruleList.forEach(runningRule -> {
                    if (minutes > 0) { // 差值一分钟以上，才会进行告警检查
                        runningRule.moveTo(checkTime);
                        /**
                         * Don't run in the first quarter per min, avoid to trigger false alarm.
                         */
                        if (checkTime.getSecondOfMinute() > 15) {
                            hasExecute[0] = true;
                            alarmMessageList.addAll(runningRule.check());
                        }
                    }
                }));
                // Set the last execute time, and make sure the second is `00`, such as: 18:30:00
                if (hasExecute[0]) { // 更新最近一次检查时间
                    lastExecuteTime = checkTime.minusSeconds(checkTime.getSecondOfMinute());
                }

                if (alarmMessageList.size() > 0) { // 将告警信息发送出去
                    allCallbacks.forEach(callback -> callback.doAlarm(alarmMessageList));
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }, 10, 10, TimeUnit.SECONDS);
    }
}
