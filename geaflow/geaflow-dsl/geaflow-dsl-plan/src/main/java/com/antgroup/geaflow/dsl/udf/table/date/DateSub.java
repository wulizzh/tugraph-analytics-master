/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.dsl.udf.table.date;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDF;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Description(name = "date_sub", description = "Returns the date that is num_days before start_date.")
public class DateSub extends UDF {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(
        "yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private final Calendar calendar = Calendar.getInstance();

    public String eval(String dateString, Integer days) {
        if (dateString == null || days == null) {
            return null;
        }
        DateTimeFormatter formatter;
        try {
            if (dateString.length() <= 10) {
                formatter = DATE_FORMATTER;
            } else {
                formatter = DATE_TIME_FORMATTER;
            }
            calendar.setTime(formatter.parseDateTime(dateString).toDate());
            calendar.add(Calendar.DAY_OF_MONTH, -days);
            Date newDate = calendar.getTime();
            return DATE_FORMATTER.print(newDate.getTime());
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public String eval(Timestamp t, Integer days) {
        if (t == null || days == null) {
            return null;
        }
        calendar.setTime(t);
        calendar.add(Calendar.DAY_OF_MONTH, -days);
        Date newDate = calendar.getTime();
        return DATE_TIME_FORMATTER.print(newDate.getTime());
    }
}
