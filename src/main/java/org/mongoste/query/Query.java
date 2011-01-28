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

import java.io.Serializable;
import java.util.List;

/**
 * Query interface
 * @author mrmx
 */
public interface Query extends Serializable {

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

}