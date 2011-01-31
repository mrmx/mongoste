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

import org.mongoste.core.StatsEngineException;
import org.mongoste.model.StatAction;
import org.mongoste.model.StatCounter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Query interface
 * @author mrmx
 */
public interface Query extends Serializable {

    /**
     * Gets the ascending order
     * @return <code>true</code> if the order is ascending <code>false</>
     * if order is descending
     */
    boolean isOrderAscending();

    /**
     * Set the global asc/descending order
     * @param orderAscending <code>true</code> to ascending order, <code>false</>
     * for descending order
     * @return This query
     */
    Query order(boolean orderAscending);

    /**
     * @return the maxResults
     */
    Integer getMaxResults();

    /**
     * Limit the results to <code>maxResults</code>
     * @param maxResults <code>maxResults</code> to limit
     * @return This query
     */
    Query limit(Integer maxResults);
   
    /**
     * Sets a default equality filter to a single field
     * @param field Field to filter by
     * @param value Value to apply
     * @return
     */
    Query filterBy(QueryField field, Object value);

    /**
     * Sets a filter to a single field with an operation
     * @param field Field to filter by
     * @param operation Filter operation
     * @param value Value to apply
     * @return
     */
    Query filterBy(QueryField field, QueryOp operation, Object value);

    /**
     * Sets a filter to a single field
     * @param field Field to filter by
     * @param filter Filter to apply
     * @return This query
     */
    Query filterBy(QueryField field, QueryFilter filter);

    /**
     * Gets a filter associated to a field
     * @param field Field to get filter
     * @return filter or <code>null</code> if field has no filter
     * @see QueryFilter
     */
    QueryFilter getFilter(QueryField field);

    /**
     * Gets a list of total performed actions
     * @return list of total performed actions
     */
    List<StatAction> getActions() throws StatsEngineException;

    /**
     * Returns the top targets for a client, target type and action
     * @return list of <code>StatCounter</code> values
     * @throws StatsEngineException
     * @see StatCounter
     */
    List<StatCounter> getTopTargets() throws StatsEngineException;

    /**
     * Returns the action counters of given target/s
     * @return a map of action->count
     * @throws StatsEngineException
     */
    Map<String, Long> getMultiTargetActionCount() throws StatsEngineException;

    /**
     * Returns the action counters of given target owner/s
     * @return a map of action->count
     * @throws StatsEngineException
     */
    Map<String, Long> getOwnerActionCount() throws StatsEngineException;

}