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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

/**
 * Various date util methods
 * @author mrmx
 */
public class DateUtil {
    public static final SimpleDateFormat FORMAT_ISO8601     = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    public static final SimpleDateFormat FORMAT_YY_MM_DD      = new SimpleDateFormat("yyyy-M-dd");

    private final static TimeZone gmtTimeZone = new SimpleTimeZone(0, "GMT");

    public static Date getDateGMT0() {
        return getCalendarGMT0().getTime();
    }

    /**
     *
     * @return Calendar with the current system date at GMT 0
     */
    public static Calendar getCalendarGMT0() {        
        return Calendar.getInstance(gmtTimeZone);
    }

    public static Calendar trimTime(Calendar cal) {
        cal.set(Calendar.HOUR,0);
        cal.set(Calendar.HOUR_OF_DAY,0);
        cal.set(Calendar.MINUTE,0);
        cal.set(Calendar.SECOND,0);
        cal.set(Calendar.MILLISECOND,0);
        return cal;
    }
}
