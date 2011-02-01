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
package org.mongoste.core.impl.mongodb;

import org.mongoste.query.QueryField;
import org.mongoste.model.StatEvent;
import org.mongoste.core.TimeScope;
import org.mongoste.model.StatAction;
import org.mongoste.model.StatCounter;
import org.mongoste.util.DateUtil;
import org.mongoste.query.Query;
import org.mongoste.query.QueryOp;
import org.mongoste.query.RequiredQueryFieldException;

import org.joda.time.DateTime;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.text.ParseException;
import java.util.LinkedList;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

/**
 * MongoStatsEngine Test
 * @author mrmx
 */
public class MongoStatsEngineTest {
    private static MongoStatsEngineEx engine;

    private static class MongoStatsEngineEx extends MongoStatsEngine {
        public StatEvent createSampleEvent() {
            return createSampleEvent(DateUtil.getDateTimeUTC());
        }
        public StatEvent createSampleEvent(String date) throws ParseException {
            Date parsedDate = null;
            try {
                parsedDate = DateUtil.FORMAT_YY_MM_DD_HH.parse(date);
            }catch(ParseException ex) {
                parsedDate = DateUtil.FORMAT_YY_MM_DD.parse(date);
            }
            return createSampleEvent(parsedDate);
        }
        public StatEvent createSampleEvent(Date date) {
            return createSampleEvent(new DateTime(date));
        }

        public StatEvent createSampleEvent(MutableDateTime date) {
            return createSampleEvent(date.toDateTime());
        }
        
        public StatEvent createSampleEvent(DateTime date) {
            StatEvent event = new StatEvent();
            event.setClientId("client");
            event.setAction("action");
            event.setTargetType("type");
            event.setTarget("target");
            event.setTargetOwners(Arrays.asList("owner1","owner2"));
            event.setTargetTags(Arrays.asList("tag1","tag2"));
            //Force sample dates to UTC
            event.setDateTime(DateUtil.toUTC(date));
            return event;
        }
    }

    public MongoStatsEngineTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        Properties props = new Properties();
        props.setProperty(MongoStatsEngine.DB_NAME, "mongoste_tests");
        engine = new MongoStatsEngineEx();
        engine.setResetCollections(true);
        engine.init(props);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        //TODO engine.shutDown();
    }

    @Before
    public void setUp() throws Exception {
        engine.dropAllCollections();
        engine.setTimeScopePrecision(MongoStatsEngine.DEFAULT_TIMESCOPE_PRECISION);
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of handleEvent method, of class MongoStatsEngine.
     */
    @Test
    public void testHandleEvent() throws Exception {
        System.out.println("handleEvent");
        engine.handleEvent(engine.createSampleEvent());
    }

    /**
     * Test of getActions method, of class MongoStatsEngine.
     */
    @Test
    public void testGetActions() throws Exception {
        System.out.println("getActions");
        StatEvent event = engine.createSampleEvent();
        engine.handleEvent(event);
        engine.handleEvent(event);
        Query query = engine.createQuery().filterBy(QueryField.CLIENT_ID,event.getClientId());
        List<StatAction> result = query.getActions();
        assertNotNull(result);
        assertEquals(1, result.size());
        StatAction action = result.get(0);
        assertNotNull(action);
        assertEquals(event.getAction(), action.getName());
        assertEquals(2, action.getCount());
        assertEquals(1, action.getTargets().size());
        StatCounter actionTarget = action.getTargets().get(0);
        assertEquals(event.getTargetType(), actionTarget.getName());
        assertEquals(2, actionTarget.getCount());
    }

    /**
     * Test of getScopeCollectionName method, of class MongoStatsEngine.
     */
    @Test
    public void testGetScopeCollectionName() {
        System.out.println("getScopeCollectionName");
        String collection = "collection";
        String expResult = collection;
        String result = engine.getScopeCollectionName(collection, (StatEvent)null, TimeScope.GLOBAL);
        System.out.println("result:"+result);
        assertEquals(expResult, result);
        Calendar cal = Calendar.getInstance();
        int year = cal.get(Calendar.YEAR);
        int week = cal.get(Calendar.WEEK_OF_YEAR);
        int month = cal.get(Calendar.MONTH) + 1 ;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        StatEvent event = new StatEvent();
        event.setDate(cal.getTime());

        TimeScope scope = TimeScope.ANNUAL;
        result = engine.getScopeCollectionName(collection, event, scope);
        System.out.println("result:"+result);
        expResult = collection + "_" + scope.getKey().toLowerCase() + year;
        assertEquals(expResult, result);

        scope = TimeScope.WEEKLY;
        result = engine.getScopeCollectionName(collection, event, scope);
        System.out.println("result:"+result);
        expResult = collection + "_" + scope.getKey().toLowerCase() + year;
        expResult = expResult + "_" + week;
        assertEquals(expResult, result);

        scope = TimeScope.MONTHLY;
        result = engine.getScopeCollectionName(collection, event, scope);
        System.out.println("result:"+result);
        expResult = collection + "_" + scope.getKey().toLowerCase() + year;
        expResult = expResult + "_" + month;
        assertEquals(expResult, result);

        scope = TimeScope.DAILY;
        result = engine.getScopeCollectionName(collection, event, scope);
        System.out.println("result:"+result);
        expResult = collection + "_" + scope.getKey().toLowerCase() + year;
        expResult = expResult + "_" + month;
        expResult = expResult + "_" + day;
        assertEquals(expResult, result);

        scope = TimeScope.HOURLY;
        result = engine.getScopeCollectionName(collection, event, scope);
        System.out.println("result:"+result);
        expResult = collection + "_" + scope.getKey().toLowerCase() + year;
        expResult = expResult + "_" + month;
        expResult = expResult + "_" + day;
        expResult = expResult + "_" + hour;
        assertEquals(expResult, result);
    }

    /**
     * Test of checkEvent method, of class MongoStatsEngine.
     */
    @Test
    public void testCheckEvent() throws Exception {
        System.out.println("checkEvent");
        StatEvent event = null;
        try {
            engine.checkEvent(event);
            fail("Expected exception");
        }catch(Exception ex) {
        }
        event = new StatEvent();
        try {
            engine.checkEvent(event);
            fail("Expected exception");
        }catch(Exception ex) {
        }        
    }

    /**
     * Test of getTopTargets method, of class MongoStatsEngine.
     */
    @Test
    public void testGetTopTargets() throws Exception {
        System.out.println("getTopTargets");
        StatEvent event = engine.createSampleEvent();
        engine.handleEvent(event);
        List<StatCounter> result = null;
        Query query = engine.createQuery();
        try {
            result = query.getTopTargets();
            fail("Required field");
        }catch(RequiredQueryFieldException ex){

        }
        query.filterBy(QueryField.CLIENT_ID, event.getClientId());
        try {
            result = query.getTopTargets();
            fail("Required field");
        }catch(RequiredQueryFieldException ex){

        }
        query.filterBy(QueryField.TARGET_TYPE, event.getTargetType());
        try {
            result = query.getTopTargets();
            fail("Required field");
        }catch(RequiredQueryFieldException ex){

        }
        query.filterBy(QueryField.ACTION, event.getAction());
        result = query.getTopTargets();
        assertNotNull(result);
        assertEquals(1, result.size());
        System.out.println("result:"+result);
        StatCounter counter = result.get(0);
        assertEquals(event.getTarget(), counter.getName());
        assertEquals(1, counter.getCount());
    }

    /**
     * Test of dropAllCollections method, of class MongoStatsEngine.
     */
    @Test
    public void testDropAllCollections() {
        System.out.println("dropAllCollections");
        engine.dropAllCollections();
    }

    @Test
    public void testTargetOwners() throws Exception {
        System.out.println("testTargetOwners");
        StatEvent event = engine.createSampleEvent();
        //Check null owner list
        event.setTargetOwners(null);
        engine.handleEvent(event);
        DBCollection targets = engine.getTargetCollection();
        assertNotNull(targets);
        assertEquals(1,targets.count());
        DBObject target = targets.find().next();
        assertNotNull(target);
        BasicDBList owners = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNull(owners);
        //Check single owner
        event.setTargetOwners(Arrays.asList("owner1"));
        engine.handleEvent(event);
        target = targets.find().next();
        assertNotNull(target);
        owners = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNotNull(owners);
        assertEquals(1,owners.size());
        assertEquals("owner1",owners.get(0));
        //Check add owners
        event.setTargetOwners(Arrays.asList("owner2"));
        engine.handleEvent(event);
        //next month
        DateTime nextMonth = event.getDateTime().plusMonths(1);
        event.setDateTime(nextMonth);
        engine.handleEvent(event);
        target = targets.find().next();
        assertNotNull(target);
        owners = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNotNull(owners);
        assertEquals(2,owners.size());
        assertEquals("owner1",owners.get(0));
        assertEquals("owner2",owners.get(1));
    }

    @Test
    public void testTargetTags() throws Exception {
        System.out.println("testTargetTags");
        StatEvent event = engine.createSampleEvent();
        //Check null tag list
        event.setTargetTags(null);
        engine.handleEvent(event);
        DBCollection targets = engine.getTargetCollection();
        assertNotNull(targets);
        assertEquals(1,targets.count());
        DBObject target = targets.find().next();
        assertNotNull(target);
        BasicDBList tags = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_TAGS);
        assertNull(tags);
        //Check single tag
        event.setTargetTags(Arrays.asList("tag1"));
        engine.handleEvent(event);
        target = targets.find().next();        
        assertNotNull(target);
        tags = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_TAGS);
        assertNotNull(tags);
        assertEquals(1,tags.size());
        assertEquals("tag1",tags.get(0));
        //Check add tag
        event.setTargetTags(Arrays.asList("tag2"));
        engine.handleEvent(event);
        //next month
        DateTime nextMonth = event.getDateTime().plusMonths(1);
        event.setDateTime(nextMonth);
        engine.handleEvent(event);
        target = targets.find().next();
        assertNotNull(target);
        tags = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_TAGS);
        assertNotNull(tags);
        assertEquals(2,tags.size());
        assertEquals("tag1",tags.get(0));
        assertEquals("tag2",tags.get(1));   
    }

    @Test
    public void testSetTargetTags() throws Exception {
        System.out.println("testSetTargetTags");
        StatEvent event = engine.createSampleEvent();
        //Create event with null tag list
        event.setTargetTags(null);
        engine.handleEvent(event);

        DBCollection targets = engine.getTargetCollection();
        assertNotNull(targets);
        assertEquals(1,targets.count());
        
        DBCollection counters = engine.getCounterCollection();
        assertNotNull(counters);
        assertEquals(1,counters.count());

        DBObject target = targets.find().next();
        assertNotNull(target);
        BasicDBList tags = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_TAGS);
        assertNull(tags);       

        DBObject targetCounter = counters.find().next();
        assertNotNull(targetCounter);
        BasicDBList counterTags = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_TAGS);

        //Check single tag
        engine.setTargetTags(event.getClientId(), event.getTargetType(),event.getTarget(), Arrays.asList("tag1"));
        target = targets.find().next();
        assertNotNull(target);
        targetCounter = counters.find().next();
        assertNotNull(targetCounter);

        tags = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_TAGS);
        assertNotNull(tags);
        assertEquals(1,tags.size());
        assertEquals("tag1",tags.get(0));
        
        counterTags = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_TAGS);
        assertNotNull(counterTags);
        assertEquals(1,counterTags.size());
        assertEquals("tag1",counterTags.get(0));

        //Check add tag
        engine.setTargetTags(event.getClientId(), event.getTargetType(),event.getTarget(), Arrays.asList("tag1","tag2"));
        target = targets.find().next();
        assertNotNull(target);
        targetCounter = counters.find().next();
        assertNotNull(targetCounter);

        tags = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_TAGS);
        assertNotNull(tags);
        assertEquals(2,tags.size());
        assertEquals("tag1",tags.get(0));
        assertEquals("tag2",tags.get(1));
        
        counterTags = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_TAGS);
        assertNotNull(counterTags);
        assertEquals(2,counterTags.size());
        assertEquals("tag1",counterTags.get(0));
        assertEquals("tag2",counterTags.get(1));

        //Check remove tag
        engine.setTargetTags(event.getClientId(), event.getTargetType(),event.getTarget(), Arrays.asList("tag2"));
        target = targets.find().next();
        assertNotNull(target);
        targetCounter = counters.find().next();
        assertNotNull(targetCounter);

        tags = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_TAGS);
        assertNotNull(tags);
        assertEquals(1,tags.size());
        assertEquals("tag2",tags.get(0));

        counterTags = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_TAGS);
        assertNotNull(counterTags);
        assertEquals(1,counterTags.size());
        assertEquals("tag2",counterTags.get(0));
    }

    @Test
    public void testSetTargetOwnersSingle() throws Exception {
        System.out.println("testSetTargetOwnersSingle");
        StatEvent event = engine.createSampleEvent();
        //Create event with null owner list
        event.setTargetOwners(null);
        engine.handleEvent(event);

        DBCollection targets = engine.getTargetCollection();
        assertNotNull(targets);
        assertEquals(1,targets.count());

        DBCollection counters = engine.getCounterCollection();
        assertNotNull(counters);
        assertEquals(1,counters.count());

        DBObject target = targets.find().next();
        assertNotNull(target);
        BasicDBList owners = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNull(owners);

        DBObject targetCounter = counters.find().next();
        assertNotNull(targetCounter);
        BasicDBList counterOwners = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNull(counterOwners);

        //Check single owner
        engine.setTargetOwners(event.getClientId(), event.getTargetType(),event.getTarget(), Arrays.asList("own1"));
        target = targets.find().next();
        assertNotNull(target);
        targetCounter = counters.find().next();
        assertNotNull(targetCounter);

        owners = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNotNull(owners);
        assertEquals(1,owners.size());
        assertEquals("own1",owners.get(0));

        counterOwners = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNotNull(counterOwners);
        assertEquals(1,counterOwners.size());
        assertEquals("own1",counterOwners.get(0));

        //Check add owner
        engine.setTargetOwners(event.getClientId(), event.getTargetType(),event.getTarget(), Arrays.asList("own1","own2"));
        target = targets.find().next();
        assertNotNull(target);
        targetCounter = counters.find().next();
        assertNotNull(targetCounter);

        owners = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNotNull(owners);
        assertEquals(2,owners.size());
        assertEquals("own1",owners.get(0));
        assertEquals("own2",owners.get(1));

        counterOwners = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNotNull(counterOwners);
        assertEquals(2,counterOwners.size());
        assertEquals("own1",counterOwners.get(0));
        assertEquals("own2",counterOwners.get(1));

        //Check remove owner
        engine.setTargetOwners(event.getClientId(), event.getTargetType(),event.getTarget(), Arrays.asList("own2"));
        target = targets.find().next();
        assertNotNull(target);
        targetCounter = counters.find().next();
        assertNotNull(targetCounter);

        owners = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNotNull(owners);
        assertEquals(1,owners.size());
        assertEquals("own2",owners.get(0));

        counterOwners = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNotNull(counterOwners);
        assertEquals(1,counterOwners.size());
        assertEquals("own2",counterOwners.get(0));
    }

    @Test
    public void testSetTargetOwnersMultiple() throws Exception {
        System.out.println("testSetTargetOwnersMultiple");
        List<String> targetList = new ArrayList<String>();
        for(int i = 1 ; i < 100 ; i++) {
            targetList.add("target"+i);
        }
        StatEvent event = engine.createSampleEvent();
        //Create event with null owner list
        event.setTargetOwners(null);
        for(String targetId : targetList) {
            event.setTarget(targetId);
            engine.handleEvent(event);
        }

        DBCollection targets = engine.getTargetCollection();
        assertNotNull(targets);
        assertEquals(targetList.size(),targets.count());

        DBCollection counters = engine.getCounterCollection();
        assertNotNull(counters);
        assertEquals(targetList.size(),counters.count());

        for(String tgt : targetList) {
            DBObject target = targets.find().next();
            assertNotNull(target);
            BasicDBList owners = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
            assertNull(owners);
            DBObject targetCounter = counters.find().next();
            assertNotNull(targetCounter);
            BasicDBList counterOwners = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
            assertNull(counterOwners);
        }

        //Check single owner
        engine.setTargetOwners(event.getClientId(), event.getTargetType(),targetList, Arrays.asList("own1"));
        for(String tgt : targetList) {
            DBObject target = targets.find().next();
            assertNotNull(target);
            DBObject targetCounter = counters.find().next();
            assertNotNull(targetCounter);
            BasicDBList owners = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
            assertNotNull(owners);
            assertEquals(1,owners.size());
            assertEquals("own1",owners.get(0));
            BasicDBList counterOwners = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
            assertNotNull(counterOwners);
            assertEquals(1,counterOwners.size());
            assertEquals("own1",counterOwners.get(0));
        }

        //Check add owner
        engine.setTargetOwners(event.getClientId(), event.getTargetType(),targetList, Arrays.asList("own1","own2"));
        for(String tgt : targetList) {
            DBObject target = targets.find().next();
            assertNotNull(target);
            DBObject targetCounter = counters.find().next();
            assertNotNull(targetCounter);

            BasicDBList owners = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
            assertNotNull(owners);
            assertEquals(2,owners.size());
            assertEquals("own1",owners.get(0));
            assertEquals("own2",owners.get(1));

            BasicDBList counterOwners = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
            assertNotNull(counterOwners);
            assertEquals(2,counterOwners.size());
            assertEquals("own1",counterOwners.get(0));
            assertEquals("own2",counterOwners.get(1));
        }

        //Check remove owner
        engine.setTargetOwners(event.getClientId(), event.getTargetType(),targetList, Arrays.asList("own2"));
        for(String tgt : targetList) {
            DBObject target = targets.find().next();
            assertNotNull(target);
            DBObject targetCounter = counters.find().next();
            assertNotNull(targetCounter);

            BasicDBList owners = (BasicDBList) target.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
            assertNotNull(owners);
            assertEquals(1,owners.size());
            assertEquals("own2",owners.get(0));

            BasicDBList counterOwners = (BasicDBList) targetCounter.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
            assertNotNull(counterOwners);
            assertEquals(1,counterOwners.size());
            assertEquals("own2",counterOwners.get(0));
        }
    }

    @Test
    public void testTargetCollectionSingle() throws Exception {
        System.out.println("testTargetCollectionSingle");
        StatEvent event = engine.createSampleEvent();
        engine.handleEvent(event);
        DBCollection targets = engine.getTargetCollection();
        assertNotNull(targets);
        assertEquals(1,targets.count());
        DBObject target = targets.find().next();
        assertNotNull(target);
        assertEquals(event.getClientId(),target.get(MongoStatsEngine.EVENT_CLIENT_ID));
        assertEquals(event.getAction(),target.get(MongoStatsEngine.EVENT_ACTION));
        assertEquals(event.getTargetType(),target.get(MongoStatsEngine.EVENT_TARGET_TYPE));
        assertEquals(event.getTarget(),target.get(MongoStatsEngine.EVENT_TARGET));
        assertEquals(event.getMonth(),target.get(MongoStatsEngine.TARGET_MONTH));
        assertEquals(event.getYear(),target.get(MongoStatsEngine.TARGET_YEAR));
        DateTime date = new DateTime((Date) target.get(MongoStatsEngine.EVENT_DATE),DateTimeZone.UTC);
        assertNotNull(date);
        assertEquals(DateUtil.trimTime(event.getDateTime()),date);
    }

    @Test
    public void testTargetCollection() throws Exception {
        System.out.println("testTargetCollection");
        MutableDateTime date = DateUtil.getDateTimeUTC(true).toMutableDateTime();
        date.setMonthOfYear(1);
        date.setDayOfMonth(1);
        System.out.println("2 events for "+date);
        StatEvent event = engine.createSampleEvent(date);
        engine.handleEvent(event);        
        event.setDate(date.toDateTime().plusHours(1));
        engine.handleEvent(event);
        //Next month
        DateTime nextMonth = date.toDateTime().plusMonths(1);
        System.out.println("1 event for "+nextMonth);
        event.setDate(nextMonth);
        engine.handleEvent(event);
        //Events in two months: 2 docs in target collection
        DBCollection targets = engine.getTargetCollection();
        assertNotNull(targets);
        assertEquals(2,targets.count());
        //Add 3th event in second month: still 2 docs in target collection
        DateTime lastMonthDay = nextMonth.plusDays(1);
        System.out.println("1 event for "+lastMonthDay);
        event.setDate(lastMonthDay);
        engine.handleEvent(event);
        targets = engine.getTargetCollection();
        assertNotNull(targets);
        assertEquals(2,targets.count());
        DBCursor dbc = targets.find().sort(new BasicDBObject(MongoStatsEngine.TARGET_MONTH, 1));
        assertNotNull(dbc);
        assertEquals(2,dbc.count());
        //First month
        BasicDBObject firstMonth = (BasicDBObject) dbc.next();
        assertEquals(2,firstMonth.get(MongoStatsEngine.FIELD_COUNT));
        BasicDBObject days = (BasicDBObject) firstMonth.get(MongoStatsEngine.FIELD_DAYS);
        assertNotNull(days);
        assertEquals(1,days.size());
        //Check target owners:
        BasicDBList owners = (BasicDBList) firstMonth.get(MongoStatsEngine.EVENT_TARGET_OWNERS);
        assertNotNull(owners);
        assertEquals(2,owners.size());
        assertEquals("owner1",owners.get(0));
        assertEquals("owner2",owners.get(1));
        //Check target tags:
        BasicDBList tags = (BasicDBList) firstMonth.get(MongoStatsEngine.EVENT_TARGET_TAGS);
        assertNotNull(tags);
        assertEquals(2,tags.size());
        assertEquals("tag1",tags.get(0));
        assertEquals("tag2",tags.get(1));
        //Last month
        BasicDBObject lastMonth = (BasicDBObject) dbc.next();
        assertEquals(nextMonth.getMonthOfYear(),lastMonth.get(MongoStatsEngine.TARGET_MONTH));
        assertEquals(nextMonth.getYear(),lastMonth.get(MongoStatsEngine.TARGET_YEAR));
        assertEquals(2,lastMonth.get(MongoStatsEngine.FIELD_COUNT));
        days = (BasicDBObject) lastMonth.get(MongoStatsEngine.FIELD_DAYS);
        assertNotNull(days);
        assertEquals(2,days.size());
        int lastDay = lastMonthDay.getDayOfMonth();
        BasicDBObject day = (BasicDBObject) days.get(String.valueOf(lastDay));
        assertNotNull(day);
        day = (BasicDBObject) days.get(String.valueOf(--lastDay));
        assertNotNull(day);
    }

    /**
     * Test of getTargetActionCount method, of class MongoStatsEngine.
     */
    @Test
    public void testGetTargetActionCount() throws Exception {
        System.out.println("getTargetActionCount");
        StatEvent event = engine.createSampleEvent();
        engine.handleEvent(event);        
        Query query = engine.createQuery();
        query.filterBy(QueryField.CLIENT_ID, event.getClientId());
        query.filterBy(QueryField.TARGET_TYPE, event.getTargetType());
        query.filterBy(QueryField.TARGET, event.getTarget());
        Map result = query.getTargetActionCount();
        assertNotNull(result);
        assertEquals(1,result.size());
        System.out.println("result:"+result);
        //TODO check collection
        List<String> targets = Arrays.asList(event.getTarget());
        query.filterBy(QueryField.TARGET,QueryOp.IN,targets);      
        result = query.getTargetActionCount();
        assertNotNull(result);
        assertEquals(1,result.size());
        System.out.println("result:"+result);
    }

    /**
     * Test of getOwnerActionCount method, of class MongoStatsEngine.
     */
    @Test
    public void testGetOwnerActionCount() throws Exception {
        System.out.println("getOwnerActionCount");
        StatEvent event = engine.createSampleEvent();
        engine.handleEvent(event);
        String owner = event.getTargetOwners().get(0);
        System.out.println("Search for owner:"+owner);
        Query query = engine.createQuery();
        query.filterBy(QueryField.CLIENT_ID, event.getClientId());
        query.filterBy(QueryField.TARGET_TYPE, event.getTargetType());
        query.filterBy(QueryField.TARGET_OWNER, event.getTargetOwners());
        Map result = query.getOwnerActionCount();
        assertNotNull(result);
        assertEquals(1,result.size());
        
        //No result
        query.filterBy(QueryField.TARGET_OWNER, "unknown-owner");
        result = query.getOwnerActionCount();
        assertNotNull(result);
        assertEquals(0,result.size());
        System.out.println("result:"+result);

        //Search with tags
        query.filterBy(QueryField.TARGET_OWNER, event.getTargetOwners());
        query.filterBy(QueryField.TARGET_TAGS, event.getTargetTags());
        result = query.getOwnerActionCount();
        assertNotNull(result);
        assertEquals(1,result.size());
        System.out.println("result:"+result);
        
        //Empty result search with unknown tag
        query.filterBy(QueryField.TARGET_TAGS, "unknown-tag");
        result = query.getOwnerActionCount();        
        assertNotNull(result);
        assertEquals(0,result.size());
        System.out.println("result:"+result);
    }

    /**
     * Test of buildStats method, of class MongoStatsEngine.
     */
    @Test
    public void testBuildStatsMonthly() throws Exception {
        System.out.println("buildStats monthly");
        StatEvent event = engine.createSampleEvent("2011-01-01");
        engine.setTimeScopePrecision(TimeScope.MONTHLY);
        engine.handleEvent(event);
        event = engine.createSampleEvent("2011-02-01");
        engine.handleEvent(event);
        engine.buildStats(TimeScope.GLOBAL,TimeScope.MONTHLY);
        //Events in two months: 2 docs in stats collection
        DBCollection stats = engine.getStatsCollection();
        assertNotNull(stats);
        assertEquals(2,stats.count());
    }

    /**
     * Test of buildStats method, of class MongoStatsEngine.
     */
    @Test
    public void testBuildStatsDaily() throws Exception {
        System.out.println("buildStats daily");
        StatEvent event = engine.createSampleEvent("2011-01-01");
        engine.setTimeScopePrecision(TimeScope.DAILY);
        engine.handleEvent(event);
        event = engine.createSampleEvent("2011-01-02");
        engine.handleEvent(event);
        engine.buildStats(TimeScope.GLOBAL,TimeScope.DAILY);
        //Events in two days: 2 docs in stats collection
        DBCollection stats = engine.getStatsCollection();
        assertNotNull(stats);
        assertEquals(2,stats.count());
    }

    /**
     * Test of buildStats method, of class MongoStatsEngine.
     */
    @Test
    public void testBuildStatsHourly() throws Exception {
        System.out.println("buildStats hourly");
        StatEvent event = engine.createSampleEvent("2011-01-01 01");
        engine.setTimeScopePrecision(TimeScope.HOURLY);
        engine.handleEvent(event);
        event = engine.createSampleEvent("2011-01-01 02");
        engine.handleEvent(event);
        engine.buildStats(TimeScope.GLOBAL,TimeScope.HOURLY);
        //Events in two hours: 2 docs in stats collection
        DBCollection stats = engine.getStatsCollection();
        assertNotNull(stats);
        assertEquals(2,stats.count());
    }

    /**
     * Test of getTargetStats method, of class MongoStatsEngine.
     */
    @Test
    public void getTargetStatsSingleMonth() throws Exception {
        System.out.println("getTargetStatsSingleMonth");
        StatEvent event1 = engine.createSampleEvent("2011-01-01");
        System.out.println("getTargetStatsSingleMonth from date "+event1.getDate());
        engine.handleEvent(event1);
        StatEvent event2  = engine.createSampleEvent("2011-01-02");
        System.out.println("getTargetStatsSingleMonth to date "+event2.getDate());
        engine.handleEvent(event2);
        Query query = engine.createQuery();
        query.filterBy(QueryField.CLIENT_ID, event1.getClientId());
        query.filterBy(QueryField.TARGET_TYPE, event1.getTargetType());
        query.filterBy(QueryField.TARGET, event1.getTarget());
        query.filterBy(QueryField.DATE_FROM, event1.getDate());
        query.filterBy(QueryField.DATE_TO, event2.getDate());
        List<StatAction> targetStats = query.getTargetStats();
        System.out.println("targetStats="+targetStats);
        assertNotNull(targetStats);
        //1 action with target stats:
        assertEquals(1,targetStats.size());
        StatAction action = targetStats.get(0);
        assertEquals(event1.getAction(),action.getName());
        assertEquals(2,action.getCount());
        assertNotNull(action.getTargets());
        //1 month of data
        assertEquals(1,action.getTargets().size());
        StatCounter counter = action.getTargets().get(0);
        assertEquals(2,counter.getCount());
    }

    /**
     * Test of getTargetStats method, of class MongoStatsEngine.
     */
    @Test
    public void getTargetStatsTwoMonths() throws Exception {
        System.out.println("getTargetStatsTwoMonths");
        StatEvent event1  = engine.createSampleEvent("2011-01-01");
        System.out.println("getTargetStatsTwoMonths from date "+event1.getDate());
        engine.handleEvent(event1);
        StatEvent event2  = engine.createSampleEvent("2011-02-01");
        System.out.println("getTargetStatsTwoMonths to date "+event2.getDate());
        engine.handleEvent(event2);
        Query query = engine.createQuery();
        query.filterBy(QueryField.CLIENT_ID, event1.getClientId());
        query.filterBy(QueryField.TARGET_TYPE, event1.getTargetType());
        query.filterBy(QueryField.TARGET, event1.getTarget());
        query.filterBy(QueryField.DATE_FROM, event1.getDate());
        query.filterBy(QueryField.DATE_TO, event2.getDate());
        List<StatAction> targetStats = query.getTargetStats();
        System.out.println("targetStats="+targetStats);
        assertNotNull(targetStats);
        //1 action with target stats:
        assertEquals(1,targetStats.size());
        StatAction action = targetStats.get(0);
        assertEquals(event1.getAction(),action.getName());
        //Global action count:
        assertEquals(2,action.getCount());
        assertNotNull(action.getTargets());
        //2 months target stats:
        assertEquals(2,action.getTargets().size());
        
        StatCounter firstMonthCounter = action.getTargets().get(0);
        assertEquals(action.getName(),firstMonthCounter.getName());
        assertEquals(1,firstMonthCounter.getDateTime().getMonthOfYear());
        assertEquals(1,firstMonthCounter.getCount());

        StatCounter secondMonthCounter = action.getTargets().get(1);
        assertEquals(action.getName(),secondMonthCounter.getName());
        assertEquals(2,secondMonthCounter.getDateTime().getMonthOfYear());
        assertEquals(1,secondMonthCounter.getCount());
    }

    /**
     * Test of getTargetStats method, of class MongoStatsEngine.
     */
    @Test
    public void getTargetStatsMultipleTargetAggregation() throws Exception {
        System.out.println("getTargetStatsMultipleTargetAggregation");
        StatEvent event1  = engine.createSampleEvent("2011-01-01");
        event1.setTarget("target1");
        engine.handleEvent(event1);
        StatEvent event2  = engine.createSampleEvent("2011-02-01");
        event2.setTarget("target2");
        engine.handleEvent(event2);
        Query query = engine.createQuery();
        query.filterBy(QueryField.CLIENT_ID, event1.getClientId());
        query.filterBy(QueryField.TARGET_TYPE, event1.getTargetType());
        query.filterBy(QueryField.TARGET, QueryOp.IN,Arrays.asList(event1.getTarget(),event2.getTarget()));
        query.filterBy(QueryField.DATE_FROM, event1.getDate());
        query.filterBy(QueryField.DATE_TO, event2.getDate());
        List<StatAction> targetStats = query.getTargetStats();
        System.out.println("targetStats="+targetStats);
        assertNotNull(targetStats);
        //1 action with target stats:
        assertEquals(1,targetStats.size());
        StatAction action = targetStats.get(0);
        assertEquals(event1.getAction(),action.getName());
        //Global action count:
        assertEquals(2,action.getCount());
        assertNotNull(action.getTargets());
        //2 months target stats:
        assertEquals(2,action.getTargets().size());

        StatCounter firstMonthCounter = action.getTargets().get(0);
        assertEquals(action.getName(),firstMonthCounter.getName());
        assertEquals(1,firstMonthCounter.getDateTime().getMonthOfYear());
        assertEquals(1,firstMonthCounter.getCount());

        StatCounter secondMonthCounter = action.getTargets().get(1);
        assertEquals(action.getName(),secondMonthCounter.getName());
        assertEquals(2,secondMonthCounter.getDateTime().getMonthOfYear());
        assertEquals(1,secondMonthCounter.getCount());
    }

    /**
     * Test of getTargetStats method, of class MongoStatsEngine.
     */
    @Test
    public void getTargetStatsMultipleActionTargetAggregation() throws Exception {
        System.out.println("getTargetStatsMultipleActionTargetAggregation");
        StatEvent event1  = engine.createSampleEvent("2011-01-01");
        event1.setAction("action1");
        event1.setTarget("target1");
        engine.handleEvent(event1);
        StatEvent event2  = engine.createSampleEvent("2011-02-01");
        event2.setAction("action2");
        event2.setTarget("target2");
        engine.handleEvent(event2);
        Query query = engine.createQuery();
        query.filterBy(QueryField.CLIENT_ID, event1.getClientId());
        query.filterBy(QueryField.TARGET_TYPE, event1.getTargetType());
        query.filterBy(QueryField.TARGET, QueryOp.IN,Arrays.asList(event1.getTarget(),event2.getTarget()));
        query.filterBy(QueryField.DATE_FROM, event1.getDate());
        query.filterBy(QueryField.DATE_TO, event2.getDate());
        List<StatAction> targetStats = query.getTargetStats();
        System.out.println("targetStats="+targetStats);
        assertNotNull(targetStats);
        //2 action with target stats:
        assertEquals(2,targetStats.size());
        for(StatAction action : targetStats) {
            //Global action count:
            assertEquals(1,action.getCount());
            assertNotNull(action.getTargets());
            //1 months target stats:
            assertEquals(1,action.getTargets().size());

            StatCounter statCounter = action.getTargets().get(0);
            assertEquals(action.getName(),statCounter.getName());            
            assertEquals(1,statCounter.getCount());

        }
    }

    /**
     * Test of getTargetStats method, of class MongoStatsEngine.
     */
    @Test
    public void getTargetStatsMultipleActionTargetAggregation1m() throws Exception {
        System.out.println("getTargetStatsMultipleActionTargetAggregation1m");
        int maxActions = 4;
        int maxTargets = 1000;
        int eventsPerMonth = maxTargets / 10;
        DateTime beginDate = DateUtil.buildUTCDate(2011,1,1);
        DateTime date;
        long t0 = System.currentTimeMillis();
        List<String> targets = new LinkedList<String>();
        for(int e = 1 ; e <= maxTargets ; e++) {
            targets.add("target"+e);
        }
        System.out.println("Start date "+beginDate);
        StatEvent event = null;
        int events = 0;
        for(int a = 1 ; a <= maxActions ; a++) {
            date = new DateTime(beginDate);
            for(int t = 1 ; t <= maxTargets ; t++) {
                event = engine.createSampleEvent(date);
                event.setAction("action"+a);
                event.setTarget("target"+t);
                engine.handleEvent(event);
                if(t % eventsPerMonth == 0) {
                    date = date.plusMonths(1);
                }
                events++;
            }
        }
        System.out.printf("Handled %d events in %d ms\n",events,System.currentTimeMillis()-t0);
        t0 = System.currentTimeMillis();
        Query query = engine.createQuery();
        query.filterBy(QueryField.CLIENT_ID, event.getClientId());
        query.filterBy(QueryField.TARGET_TYPE, event.getTargetType());
        query.filterBy(QueryField.TARGET, QueryOp.IN,targets);
        query.filterBy(QueryField.DATE_FROM, beginDate);        
        List<StatAction> targetStats = query.getTargetStats();
        System.out.printf("Query took %d ms\n",System.currentTimeMillis()-t0);
        assertNotNull(targetStats);
        //action with target stats:
        assertEquals(maxActions,targetStats.size());
        for(StatAction action : targetStats) {
            //Global action count:        
            assertNotNull(action.getTargets());
            System.out.println("Action "+action.getName()+ " count= "+action.getCount());
            System.out.println("Action "+action.getName() + " has " + action.getTargets().size() + " targets");
            long total = 0;
            for(StatCounter counter : action.getTargets()) {
                total += counter.getCount();
            }
            assertEquals(action.getCount(), total);
        }
    }

}