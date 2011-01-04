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

import org.mongoste.model.StatEvent;
import org.mongoste.core.TimeScope;
import org.mongoste.model.StatAction;
import org.mongoste.model.StatBasicCounter;
import org.mongoste.util.DateUtil;

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

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * MongoStatsEngine Test
 * @author mrmx
 */
public class MongoStatsEngineTest {
    private static MongoStatsEngineEx engine;

    private static class MongoStatsEngineEx extends MongoStatsEngine {
        public StatEvent createSampleEvent() {
            StatEvent event = new StatEvent();
            event.setClientId("client");
            event.setAction("action");
            event.setTargetType("type");
            event.setTarget("target");
            event.setTargetOwners(Arrays.asList("owner1","owner2"));
            event.setTargetTags(Arrays.asList("tag1","tag2"));
            event.setDate(DateUtil.getDateGMT0());
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
    public void setUp() {
        engine.dropAllCollections();
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of logEvent method, of class MongoStatsEngine.
     */
    @Test
    public void testLogEvent() throws Exception {
        System.out.println("logEvent");
        engine.logEvent(engine.createSampleEvent());
    }

    /**
     * Test of buildStats method, of class MongoStatsEngine.
     */
    @Test
    public void testBuildStats() {
        System.out.println("buildStats");
        //fail("The test case is a prototype.");
    }

    /**
     * Test of getActions method, of class MongoStatsEngine.
     */
    @Test
    public void testGetActions() throws Exception {
        System.out.println("getActions");
        StatEvent event = engine.createSampleEvent();
        engine.logEvent(event);
        engine.logEvent(event);
        List<StatAction> result = engine.getActions(event.getClientId());
        assertNotNull(result);
        assertEquals(1, result.size());
        StatAction action = result.get(0);
        assertNotNull(action);
        assertEquals(event.getAction(), action.getName());
        assertEquals(2, action.getCount());
        assertEquals(1, action.getTargets().size());
        StatBasicCounter actionTarget = action.getTargets().get(0);
        assertEquals(event.getTargetType(), actionTarget.getName());
        assertEquals(2, actionTarget.getCount());
    }

    /**
     * Test of getTargetScopeCollectionName method, of class MongoStatsEngine.
     */
    @Test
    public void testGetTargetScopeCollectionName() {
        System.out.println("getTargetScopeCollectionName");
        String collection = "collection";
        String expResult = collection;
        String result = engine.getTargetScopeCollectionName(collection, null, TimeScope.GLOBAL);
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
        result = engine.getTargetScopeCollectionName(collection, event, scope);
        System.out.println("result:"+result);
        expResult = collection + "_" + scope.getKey().toLowerCase() + year;
        assertEquals(expResult, result);

        scope = TimeScope.WEEKLY;
        result = engine.getTargetScopeCollectionName(collection, event, scope);
        System.out.println("result:"+result);
        expResult = collection + "_" + scope.getKey().toLowerCase() + year;
        expResult = expResult + "_" + week;
        assertEquals(expResult, result);

        scope = TimeScope.MONTHLY;
        result = engine.getTargetScopeCollectionName(collection, event, scope);
        System.out.println("result:"+result);
        expResult = collection + "_" + scope.getKey().toLowerCase() + year;
        expResult = expResult + "_" + month;
        assertEquals(expResult, result);

        scope = TimeScope.DAILY;
        result = engine.getTargetScopeCollectionName(collection, event, scope);
        System.out.println("result:"+result);
        expResult = collection + "_" + scope.getKey().toLowerCase() + year;
        expResult = expResult + "_" + month;
        expResult = expResult + "_" + day;
        assertEquals(expResult, result);

        scope = TimeScope.HOURLY;
        result = engine.getTargetScopeCollectionName(collection, event, scope);
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
        engine.logEvent(event);
        List<StatBasicCounter> result = engine.getTopTargets(event.getClientId(), event.getTargetType(), event.getAction(), null);
        assertNotNull(result);
        assertEquals(1, result.size());
        System.out.println("result:"+result);
        StatBasicCounter counter = result.get(0);
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
        engine.logEvent(event);
        DBCollection targets = engine.getFullTargetCollection((StatEvent)null, TimeScope.GLOBAL);
        assertNotNull(targets);
        assertEquals(1,targets.count());
        DBObject target = targets.find().next();
        assertNotNull(target);
        BasicDBList owners = (BasicDBList) target.get(engine.EVENT_TARGET_OWNERS);
        assertNull(owners);
        //Check single owner
        event.setTargetOwners(Arrays.asList("owner1"));
        engine.logEvent(event);
        target = targets.find().next();
        assertNotNull(target);
        owners = (BasicDBList) target.get(engine.EVENT_TARGET_OWNERS);
        assertNotNull(owners);
        assertEquals(1,owners.size());
        assertEquals("owner1",owners.get(0));
        //Check add owners
        event.setTargetOwners(Arrays.asList("owner2"));
        engine.logEvent(event);
        //next month
        Calendar cal = DateUtil.trimTime(DateUtil.getCalendarGMT0());
        cal.add(Calendar.MONTH,1);
        event.setDate(cal.getTime());
        engine.logEvent(event);
        target = targets.find().next();
        assertNotNull(target);
        owners = (BasicDBList) target.get(engine.EVENT_TARGET_OWNERS);
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
        engine.logEvent(event);
        DBCollection targets = engine.getFullTargetCollection((StatEvent)null, TimeScope.GLOBAL);
        assertNotNull(targets);
        assertEquals(1,targets.count());
        DBObject target = targets.find().next();
        assertNotNull(target);
        BasicDBList tags = (BasicDBList) target.get(engine.EVENT_TARGET_TAGS);
        assertNull(tags);
        //Check single tag
        event.setTargetTags(Arrays.asList("tag1"));
        engine.logEvent(event);
        target = targets.find().next();        
        assertNotNull(target);
        tags = (BasicDBList) target.get(engine.EVENT_TARGET_TAGS);
        assertNotNull(tags);
        assertEquals(1,tags.size());
        assertEquals("tag1",tags.get(0));
        //Check add tag
        event.setTargetTags(Arrays.asList("tag2"));
        engine.logEvent(event);
        //next month
        Calendar cal = DateUtil.trimTime(DateUtil.getCalendarGMT0());
        cal.add(Calendar.MONTH,1);
        event.setDate(cal.getTime());
        engine.logEvent(event);
        target = targets.find().next();
        assertNotNull(target);
        tags = (BasicDBList) target.get(engine.EVENT_TARGET_TAGS);
        assertNotNull(tags);
        assertEquals(2,tags.size());
        assertEquals("tag1",tags.get(0));
        assertEquals("tag2",tags.get(1));   
    }

    @Test
    public void testTargetCollection() throws Exception {
        System.out.println("testTargetCollection");
        Calendar cal = DateUtil.trimTime(DateUtil.getCalendarGMT0());
        //cal.set(Calendar.MONTH, Calendar.JANUARY);
        System.out.println("2 events for "+cal.getTime());
        StatEvent event = engine.createSampleEvent();
        event.setDate(cal.getTime());
        engine.logEvent(event);
        cal.add(Calendar.HOUR_OF_DAY,1);
        event.setDate(cal.getTime());
        engine.logEvent(event);
        cal.add(Calendar.MONTH,1);
        System.out.println("1 event for "+cal.getTime());
        event.setDate(cal.getTime());        
        engine.logEvent(event);
        //Events in two months: 2 docs in target collection
        DBCollection targets = engine.getFullTargetCollection((StatEvent)null, TimeScope.GLOBAL);
        assertNotNull(targets);
        assertEquals(2,targets.count());
        //Add 3th event in second month: still 2 docs in target collection
        cal.add(Calendar.DATE,1);
        System.out.println("1 event for "+cal.getTime());
        event.setDate(cal.getTime());
        engine.logEvent(event);
        targets = engine.getFullTargetCollection((StatEvent)null, TimeScope.GLOBAL);
        assertNotNull(targets);
        assertEquals(2,targets.count());
        DBCursor dbc = targets.find();
        assertNotNull(dbc);
        assertEquals(2,dbc.count());
        //First month
        BasicDBObject firstMonth = (BasicDBObject) dbc.next();
        assertEquals(2,firstMonth.get(engine.FIELD_COUNT));
        BasicDBObject days = (BasicDBObject) firstMonth.get(engine.FIELD_DAYS);
        assertNotNull(days);
        assertEquals(1,days.size());
        //Check target owners:
        BasicDBList owners = (BasicDBList) firstMonth.get(engine.EVENT_TARGET_OWNERS);
        assertNotNull(owners);
        assertEquals(2,owners.size());
        assertEquals("owner1",owners.get(0));
        assertEquals("owner2",owners.get(1));
        //Check target tags:
        BasicDBList tags = (BasicDBList) firstMonth.get(engine.EVENT_TARGET_TAGS);
        assertNotNull(tags);
        assertEquals(2,tags.size());
        assertEquals("tag1",tags.get(0));
        assertEquals("tag2",tags.get(1));
        //Last month
        BasicDBObject lastMonth = (BasicDBObject) dbc.next();
        assertEquals(cal.get(Calendar.MONTH) + 1,lastMonth.get(engine.TARGET_MONTH));
        assertEquals(cal.get(Calendar.YEAR),lastMonth.get(engine.TARGET_YEAR));
        assertEquals(2,lastMonth.get(engine.FIELD_COUNT));
        days = (BasicDBObject) lastMonth.get(engine.FIELD_DAYS);
        assertNotNull(days);
        assertEquals(2,days.size());
        int lastDay = cal.get(Calendar.DATE);
        BasicDBObject day = (BasicDBObject) days.get(String.valueOf(lastDay));
        assertNotNull(day);
        day = (BasicDBObject) days.get(String.valueOf(--lastDay));
        assertNotNull(day);
    }

    /**
     * Test of getMultiTargetActionCount method, of class MongoStatsEngine.
     */
    @Test
    public void testGetMultiTargetActionCount() throws Exception {
        System.out.println("getMultiTargetActionCount");
        StatEvent event = engine.createSampleEvent();
        engine.logEvent(event);
        List<String> targets = Arrays.asList(event.getTarget());
        Map result = engine.getMultiTargetActionCount(event.getClientId(), event.getTargetType(), targets);
        assertNotNull(result);
        assertEquals(1,result.size());
        System.out.println("result:"+result);
    }

}