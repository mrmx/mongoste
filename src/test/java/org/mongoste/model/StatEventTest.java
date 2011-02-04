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
package org.mongoste.model;

import org.mongoste.util.DateUtil;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Calendar;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Test StatEvent 
 * @author mrmx
 */
public class StatEventTest {
    private StatEvent event;

    public StatEventTest() {
    }


    @Before
    public void setUp() throws ParseException {
        event = createSampleEvent("2011-01-01");
    }

    @Test
    public void testUTCTimezone() throws Exception {
        assertEquals(DateTimeZone.UTC,event.getYearMonthDate().getZone());
        assertEquals(DateTimeZone.UTC,event.getDateTime().getZone());
    }

    /**
     * Test of getYearMonthDate method, of class StatEvent.
     */
    @Test
    public void testGetYearMonthDate() throws Exception {
        System.out.println("getYearMonthDate");
        System.out.println("getYearMonthDate date:"+event.getDate());        
        DateTime result = event.getYearMonthDate();
        assertNotNull(result);
        assertEquals(1293840000000L, result.getMillis());
        assertEquals(2011, result.getYear());
        assertEquals(1, result.getMonthOfYear());
        assertEquals(1, result.getDayOfMonth());
        assertEquals(0, result.getHourOfDay());
        assertEquals(0, result.getMinuteOfHour());
        assertEquals(0, result.getSecondOfMinute());
        assertEquals(0, result.getMillisOfSecond());
    }

    /**
     * Test of getYear method, of class StatEvent.
     */
    @Test
    public void testGetYear() {
        System.out.println("getYear");        
        int expResult = 2011;
        int result = event.getYear();
        assertEquals(expResult, result);
    }

    /**
     * Test of getWeek method, of class StatEvent.
     */
    @Test
    public void testGetWeek() throws ParseException {
        System.out.println("getWeek");        
        int expResult = 52;
        int result = event.getWeek();
        assertEquals(expResult, result);
        expResult = 1;
        result = createSampleEvent("2003-01-01").getWeek();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMonth method, of class StatEvent.
     */
    @Test
    public void testGetMonth() {
        System.out.println("getMonth");
        int expResult = 1;
        int result = event.getMonth();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDay method, of class StatEvent.
     */
    @Test
    public void testGetDay() {
        System.out.println("getDay");        
        int expResult = 1;
        int result = event.getDay();
        assertEquals(expResult, result);
    }

    /**
     * Test of getHour method, of class StatEvent.
     */
    @Test
    public void testGetHour() throws ParseException {
        System.out.println("getHour");
        StatEvent instance = createSampleEvent("2011-01-01 01");
        int expResult = 1;
        int result = instance.getHour();
        assertEquals(expResult, result);
    }


    private StatEvent createSampleEvent(String date) throws ParseException {
        Date parsedDate = null;
        try {
            parsedDate = DateUtil.FORMAT_YY_MM_DD_HH.parse(date);
        } catch (ParseException ex) {
            parsedDate = DateUtil.FORMAT_YY_MM_DD.parse(date);
        }
        return createSampleEvent(parsedDate);
    }

    private StatEvent createSampleEvent(Date date) {
        StatEvent statEvent = new StatEvent();
        statEvent.setClientId("client");
        statEvent.setAction("action");
        statEvent.setTargetType("type");
        statEvent.setTarget("target");
        statEvent.setTargetOwners(Arrays.asList("owner1", "owner2"));
        statEvent.setTargetTags(Arrays.asList("tag1", "tag2"));
        statEvent.setDateTime(DateUtil.toUTC(date));
        return statEvent;
    }
}