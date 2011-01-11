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

import org.mongoste.model.StatEvent;
import org.mongoste.model.StatAction;
import org.mongoste.model.StatBasicCounter;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Stats engine interface
 */
public interface StatsEngine {

    public void setKeepEvents(boolean keepEvents);
    public boolean isKeepEvents();
    public void setCountEvents(boolean countEvents);
    public boolean isCountEvents();
    
    public void init(Properties properties) throws StatsEngineException;
    public void logEvent(StatEvent event) throws StatsEngineException;
    public void buildStats() throws StatsEngineException;

    public List<StatAction> getActions(String clientId) throws StatsEngineException;
    public List<StatBasicCounter> getTopTargets(String clientId,String targetType,String action,Integer limit) throws StatsEngineException;
    public Map<String,Long> getMultiTargetActionCount(String clientId,String targetType,List<String> targets) throws StatsEngineException;
    public Map<String,Long> getOwnerActionCount(String clientId,String targetType,String owner,String... tags) throws StatsEngineException;

    public void setTargetOwners(String clientId,String targetType,String target,List<String> owners) throws StatsEngineException;
    public void setTargetTags(String clientId,String targetType,String target,List<String> tags) throws StatsEngineException;
}