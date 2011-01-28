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

import java.util.Collection;
import org.apache.commons.lang.StringUtils;

/**
 * Query filter
 * @author mrmx
 */
public class QueryFilter {    
    private QueryOp operation;
    private Object value;

    /**
     * Constructs a query filter
     * @param operation Filter operation
     * @param value Value to filter by
     */
    public QueryFilter(QueryOp operation, Object value) {
        this.operation = operation;
        this.value = value;
    }

    /**
     * @return the operation
     */
    public QueryOp getOperation() {
        return operation;
    }

    /**
     * @param operation the operation to set
     */
    public void setOperation(QueryOp operation) {
        this.operation = operation;
    }

    /**
     * @return the value
     */
    public Object getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(Object value) {
        this.value = value;
    }

    /**
     * Check if the value of this filter is empty
     * @return <code>true</code> if empty or <code>false</code> otherwise
     */
    public boolean isEmpty() {
        if(value == null) {
            return true;
        }else if(value instanceof String) {
            return StringUtils.isEmpty(value.toString());
        }else if(value instanceof Collection) {
            return ((Collection)value).isEmpty();
        }else if(value instanceof Object[]) {
            return ((Object[])value).length > 0;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("[");
        sb.append(operation).append(" ").append(value);
        sb.append("]");
        return sb.toString();
    }

}
