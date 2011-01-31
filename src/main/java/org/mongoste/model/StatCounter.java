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
package org.mongoste.model;

import java.util.Date;

/**
 * A stat counter
 * @author mrmx
 */
public class StatCounter {
    private String name;
    private long count;
    private Date date;

    public StatCounter(String name, long count) {
        this(name,count,null);
    }

    public StatCounter(String name, long count, Date date) {
        this.name = name;
        this.count = count;
        this.date = date;
    }


    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the count
     */
    public long getCount() {
        return count;
    }

    /**
     * @param count the count to set
     */
    public void setCount(long count) {
        this.count = count;
    }

    /**
     * @return the date
     */
    public Date getDate() {
        return date;
    }

    /**
     * @param date the date to set
     */
    public void setDate(Date date) {
        this.date = date;
    }

    /**
     * @param count the count to add
     */
    public void add(long count) {
        this.count += count;
    }


    @Override
    public String toString() {
        return getClass().getSimpleName()+"["+getName()+":"+getCount()+"]";
    }


}
