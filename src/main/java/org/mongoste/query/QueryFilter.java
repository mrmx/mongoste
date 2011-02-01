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

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Date;

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
     * Return the value as <code>Integer</code>
     * @return the value as <code>Integer</code> or <code>null</code>
     * if can not be converted
     * @see Integer
     */
    public Integer getIntegerValue() {
        if(value == null) {
            return null;
        }
        Integer result = null;
        if(value instanceof Number) {
            if(value instanceof Integer) {
                result = (Integer)value;
            } else {
                result = ((Number)value).intValue();
            }
        }
        if(result == null) {
            try {
                result = Integer.valueOf(getStringValue());
            }catch(NumberFormatException ex) {

            }
        }
        return result;
    }

/**
     * Return the value as <code>Long</code>
     * @return the value as <code>Long</code> or <code>null</code>
     * if can not be converted
     * @see Long
     */
    public Long getLongValue() {
        if(value == null) {
            return null;
        }
        Long result = null;
        if(value instanceof Number) {
            if(value instanceof Long) {
                result = (Long)value;
            } else {
                result = ((Number)value).longValue();
            }
        }
        if(result == null) {
            try {
                result = Long.valueOf(getStringValue());
            }catch(NumberFormatException ex) {

            }
        }
        return result;
    }

    /**
     * Return the value as <code>String</code>
     * @return the value as <code>String</code> or <code>null</code>
     * if can not be converted
     * @see String
     */
    public String getStringValue() {
        if(value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    /**
     * Return the value as <code>Date</code>
     * @return the value as <code>Date</code> or <code>null</code>
     * if can not be converted
     * @see Date
     */
    public Date getDateValue() {
        if(value == null) {
            return null;
        }
        if(value instanceof Date) {
            return (Date)value;
        }
        return null;
    }

    /**
     * Return the value  as <code>DateTime</code>
     * @return the value as <code>DateTime</code> or <code>null</code>
     * if can not be converted
     * @see DateTime
     */
    public DateTime getDateTimeValue() {
        if(value == null) {
            return null;
        }
        if(value instanceof DateTime) {
            return (DateTime)value;
        }
        Date date = getDateValue();
        return date == null ? null : new DateTime(date);
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
