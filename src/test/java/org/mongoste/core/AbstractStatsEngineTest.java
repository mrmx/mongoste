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
package org.mongoste.core;

import org.mongoste.model.StatAction;
import org.mongoste.model.StatCounter;
import org.mongoste.model.StatEvent;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.mongoste.query.Query;

/**
 *
 * @author mrmx
 */
public class AbstractStatsEngineTest {
    AbstractStatsEngine instance;

    @Before
    public void setUp() {
        instance = new AbstractStatsEngineImpl();
    }

    /**
     * Test of createDotPath method, of class AbstractStatsEngine.
     */
    @Test
    public void testCreateDotPath() {
        System.out.println("createDotPath");
        assertEquals("a", instance.createDotPath("a"));
        assertEquals("a.b", instance.createDotPath("a","b"));
        assertEquals("a.b.c", instance.createDotPath("a","b","c"));
        assertEquals("a.c", instance.createDotPath("a",null,"c"));
        assertEquals("a.c", instance.createDotPath("a","","c"));
        assertEquals("1.2", instance.createDotPath(1,2));
        assertEquals("a.c", instance.createDotPath("a.","c"));
        assertEquals("a.c", instance.createDotPath("a..","c."));
        assertEquals("a.c", instance.createDotPath("a",".","c"));
    }

    public class AbstractStatsEngineImpl extends AbstractStatsEngine {

        @Override
        public void init(Properties properties) throws StatsEngineException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List<TimeScope> getSupportedTimeScopePrecision() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void handleEvent(StatEvent event) throws StatsEngineException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void buildStats(TimeScope scope,TimeScope groupByScope) throws StatsEngineException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List<StatAction> getActions(Query query) throws StatsEngineException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List<StatCounter> getTopTargets(Query query) throws StatsEngineException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Map<String, Long> getTargetActionCount(Query query) throws StatsEngineException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Map<String, Long> getOwnerActionCount(Query query) throws StatsEngineException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setTargetOwners(String clientId, String targetType, List<String> targets, List<String> owners) throws StatsEngineException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setTargetTags(String clientId, String targetType, String target, List<String> tags) throws StatsEngineException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

}