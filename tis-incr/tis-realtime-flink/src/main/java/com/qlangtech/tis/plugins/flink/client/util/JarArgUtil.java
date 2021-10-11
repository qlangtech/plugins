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

package com.qlangtech.tis.plugins.flink.client.util;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-10 18:02
 **/
public class JarArgUtil {

    private static final Pattern ARGUMENTS_TOKENIZE_PATTERN = Pattern.compile("([^\"\']\\S*|\".+?\"|\'.+?\')\\s*");

    /**
     * Takes program arguments as a single string, and splits them into a list of string.
     *
     * <pre>
     * tokenizeArguments("--foo bar")            = ["--foo" "bar"]
     * tokenizeArguments("--foo \"bar baz\"")    = ["--foo" "bar baz"]
     * tokenizeArguments("--foo 'bar baz'")      = ["--foo" "bar baz"]
     * tokenizeArguments(null)                   = []
     * </pre>
     *
     * <strong>WARNING: </strong>This method does not respect escaped quotes.
     */
    public static List<String> tokenizeArguments(@Nullable final String args) {
        if (args == null) {
            return Collections.emptyList();
        }
        final Matcher matcher = ARGUMENTS_TOKENIZE_PATTERN.matcher(args);
        final List<String> tokens = new ArrayList<>();
        while (matcher.find()) {
            tokens.add(matcher.group()
                    .trim()
                    .replace("\"", "")
                    .replace("\'", ""));
        }
        return tokens;
    }
}
