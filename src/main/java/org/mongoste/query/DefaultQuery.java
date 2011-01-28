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
    private StatsEngine statsEngine;
    private Integer maxResults;
    private Map<QueryField,QueryFilter> filterByMap;

    /**
     *
     * @param statsEngine
     */
    public DefaultQuery(StatsEngine statsEngine) {
        this.statsEngine = statsEngine;
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
        return filterBy(field,new QueryFilter(QueryOp.EQ,value));
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

}
