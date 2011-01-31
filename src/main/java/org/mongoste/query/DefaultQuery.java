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
package org.mongoste.query;

import org.mongoste.core.StatsEngine;
import org.mongoste.core.StatsEngineException;
import org.mongoste.model.StatAction;
import org.mongoste.model.StatCounter;
import static org.mongoste.query.QueryField.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.EnumMap;

/**
 * Default Query implementation
 *
 * @author mrmx
 */
public class DefaultQuery implements Query {
    private static Logger log = LoggerFactory.getLogger(DefaultQuery.class);

    private StatsEngine statsEngine;
    private Integer maxResults;
    private Map<QueryField,QueryFilter> filterByMap;
    private boolean orderAscending;

    /**
     *
     * @param statsEngine
     */
    public DefaultQuery(StatsEngine statsEngine) {
        this.statsEngine = statsEngine;
    }

    /**
     * Gets the ascending order
     * @return <code>true</code> if the order is ascending <code>false</>
     * if order is descending
     */
    @Override
    public boolean isOrderAscending() {
        return orderAscending;
    }

    /**
     * Set the global asc/descending order
     * @param orderAscending <code>true</code> to ascending order, <code>false</>
     * for descending order
     * @return This query
     */
    @Override
    public Query order(boolean orderAscending) {
        this.orderAscending = orderAscending;
        return this;
    }

    /**
     * @return the maxResults
     */
    @Override
    public Integer getMaxResults() {
        return maxResults;
    }

    /**
     * Limit the results to <code>maxResults</code>
     * @param maxResults <code>maxResults</code> to limit
     * @return This query
     */
    @Override
    public Query limit(Integer maxResults) {
        this.maxResults = maxResults;
        return this;
    }

    /**
     * Sets a default equality filter to a single field
     * @param field Field to filter by
     * @param value Value to apply
     * @return
     */
    @Override
    public Query filterBy(QueryField field,Object value) {
        return filterBy(field,QueryOp.EQ,value);
    }

    
    /**
     * Sets a filter to a single field with an operation
     * @param field Field to filter by
     * @param operation Filter operation
     * @param value Value to apply
     * @return
     */
    @Override
    public Query filterBy(QueryField field,QueryOp operation,Object value) {
        return filterBy(field,new QueryFilter(operation,value));
    }
    
    /**
     * Sets a filter to a single field
     * @param field Field to filter by
     * @param filter Filter to apply
     * @return This query
     */
    @Override
    public Query filterBy(QueryField field,QueryFilter filter) {
        getFilterByMap().put(field, filter);
        return this;
    }

    /**
     * Gets a filter associated to a field
     * @param field Field to get filter
     * @return filter or <code>null</code> if field has no filter
     * @see QueryFilter
     */
    @Override
    public QueryFilter getFilter(QueryField field) {
        return getFilterByMap().get(field);
    }

    /**************************************************************************/

    /**
     * Gets a list of total performed actions
     * @return list of total performed actions
     * @throws StatsEngineException
     */
    @Override
    public List<StatAction> getActions() throws StatsEngineException {
        return statsEngine.getActions(this);
    }
    
    /**
     * Returns the top targets for a client, target type and action
     * @return list of <code>StatCounter</code> values
     * @throws StatsEngineException
     * @see StatCounter
     */
    @Override
    public List<StatCounter> getTopTargets() throws StatsEngineException {
        assertNotEmpty(CLIENT_ID,TARGET_TYPE,ACTION);
        log.debug("getTopTargets query {}",this);
        return statsEngine.getTopTargets(this);
    }

    
    /**
     * Returns the action counters of given target/s
     * @return a map of action->count
     * @throws StatsEngineException
     */
    @Override
    public Map<String,Long> getMultiTargetActionCount() throws StatsEngineException {
        assertNotEmpty(CLIENT_ID,TARGET_TYPE,TARGET);
        log.debug("getMultiTargetActionCount query {}",this);
        return statsEngine.getMultiTargetActionCount(this);
    }

    /**
     * Returns the action counters of given target owner/s
     * @return a map of action->count
     * @throws StatsEngineException
     */
    @Override
    public Map<String,Long> getOwnerActionCount() throws StatsEngineException {
        assertNotEmpty(CLIENT_ID,TARGET_TYPE,TARGET_OWNER);
        log.debug("getOwnerActionCount query {}",this);
        return statsEngine.getOwnerActionCount(this);
    }


    /**
     * Checks if the provided fields has non-empty filters associated
     * @param fields Fields to check
     * @throws RequiredQueryFieldException if field has no filter or filter is empty
     */
    protected void assertNotEmpty(QueryField ... fields) throws RequiredQueryFieldException {
        QueryFilter filter;
        for(QueryField field : fields) {
            filter = getFilterByMap().get(field);
            if(filter == null || filter.isEmpty()) {
                throw new RequiredQueryFieldException(field);
            }
        }
    }

    /**
     * Gets the filterBy map
     * @return filterBy map
     */
    protected Map<QueryField, QueryFilter> getFilterByMap() {
        synchronized(this) {
            if(filterByMap == null) {
                filterByMap = Collections.synchronizedMap(
                        new EnumMap<QueryField,QueryFilter>(QueryField.class)
                );
            }
        }
        return filterByMap;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("[");
        sb.append("filterBy:").append(getFilterByMap());
        sb.append("]");
        return sb.toString();
    }

}