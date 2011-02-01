/*
 *    Copyright (c) 2010-2011 Manuel Polo (mrmx.org)
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.mongoste.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * Various date util methods
 * @author mrmx
 */
public class DateUtil {
    public static final SimpleDateFormat FORMAT_ISO8601     = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    public static final SimpleDateFormat FORMAT_YY_MM_DD    = new SimpleDateFormat("yyyy-M-dd");
    public static final SimpleDateFormat FORMAT_YY_MM_DD_HH = new SimpleDateFormat("yyyy-M-dd HH");

    /**
     * Get DateTime with the current system date in UTC time zone
     * @param trimTime if <code>true</code> then trim time data (zeroing it)
     * @return DateTime with the current system date in UTC time zone
     */
    public static DateTime getDateTimeUTC(boolean trimTime) {
        DateTime dateTime = getDateTimeUTC();
        if(trimTime) {
            dateTime = trimTime(dateTime);
        }
        return dateTime;
    }

    /**
     * Get DateTime with the current system date in UTC time zone
     * @return DateTime with the current system date in UTC time zone
     */
    public static DateTime getDateTimeUTC() {
        return new DateTime(DateTimeZone.UTC);
    }

    public static DateTime trimTime(DateTime dateTime) {
        MutableDateTime mdt = dateTime.toMutableDateTime();
        mdt.setTime(0, 0, 0, 0);
        return mdt.toDateTime();
    }

    public static DateTime toUTC(Date date) {
        return toUTC(new DateTime(date));
    }

    public static DateTime toUTC(DateTime fromDate) {
        if(DateTimeZone.UTC.equals(fromDate.getZone())) {
            return fromDate;
        }
        MutableDateTime dt = getDateTimeUTC().toMutableDateTime();
        dt.setDateTime(
            fromDate.getYear(),
            fromDate.getMonthOfYear(),
            fromDate.getDayOfMonth(),
            fromDate.getHourOfDay(),
            fromDate.getMinuteOfHour(),
            fromDate.getSecondOfMinute(),
            fromDate.getMillisOfSecond()
        );
        return  dt.toDateTime();
    }
}
