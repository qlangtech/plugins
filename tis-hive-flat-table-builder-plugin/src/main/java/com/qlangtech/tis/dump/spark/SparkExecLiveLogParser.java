/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.dump.spark;

import com.qlangtech.tis.dump.IExecLiveLogParser;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.phasestatus.JobLog;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: baisui 百岁
 * @create: 2020-06-16 14:23
 **/
public class SparkExecLiveLogParser implements IExecLiveLogParser {

    private static final String STATUS_PREFIX = "\u0001";

    private static final Pattern RUNNING_STATUS = Pattern.compile("\\u0001jobid:(\\d+?),stageid:(\\d+?),alltask:(\\d+?),complete:(\\d+?),percent:(\\d+?)");
    private static final Pattern COMPLETE_STATUS = Pattern.compile("\\u0001complete jobid:(\\d+?),state:(.+?)");
    private static final Pattern START_STATUS = Pattern.compile("\\u0001start jobid:(\\d+?)");


    private static final String EXEC_RESULT_SUCCESS = "success";
    private static final String EXEC_RESULT_FAILD = "faild";

    private final IJoinTaskStatus joinTaskStatus;
    private boolean execOver = false;

    public SparkExecLiveLogParser(IJoinTaskStatus joinTaskStatus) {
        this.joinTaskStatus = joinTaskStatus;
    }

    @Override
    public void process(String log) {
        if (!StringUtils.startsWith(log, STATUS_PREFIX)) {
            return;
        }
        Integer jobId = null;
        Matcher matcher = RUNNING_STATUS.matcher(log);
        Integer percent = null;
        if (matcher.matches()) {
            jobId = Integer.parseInt(matcher.group(1));
            percent = Integer.parseInt(matcher.group(5));
            JobLog job = joinTaskStatus.getJoblog(jobId);
            if (job == null) {
                return;
            }
            job.setWaiting(false);
            job.setReducer(percent);
            job.setMapper(percent);
            return;
        }
        matcher = START_STATUS.matcher(log);
        if (matcher.matches()) {
            jobId = Integer.parseInt(matcher.group(1));
            joinTaskStatus.createJobStatus(jobId);
            joinTaskStatus.setStart();
            return;
        }

        matcher = COMPLETE_STATUS.matcher(log);
        if (matcher.matches()) {
            this.execOver = true;
            jobId = Integer.parseInt(matcher.group(1));
            String result = matcher.group(2);
            joinTaskStatus.setComplete(true);
            joinTaskStatus.setFaild(!EXEC_RESULT_SUCCESS.equals(result));
            return;
        }

    }

    @Override
    public boolean isExecOver() {
        return this.execOver;
    }
}
