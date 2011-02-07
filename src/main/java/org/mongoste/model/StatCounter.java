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

import org.joda.time.DateTime;

import java.util.Date;
import java.io.Serializable;

/**
 * A stat counter
 * @author mrmx
 */
public class StatCounter implements Serializable {
    
    /**
	 * Serial version
	 */
	private static final long serialVersionUID = -3881894514971648160L;
	private String name;
    private long count;
    private DateTime dateTime;

    public StatCounter(String name, long count) {
        this(name,count,(DateTime)null);
    }

    public StatCounter(String name, long count, Date date) {
        this(name,count,new DateTime(date));
    }

    public StatCounter(String name, long count, DateTime dateTime) {
        this.name = name;
        this.count = count;
        this.dateTime = dateTime;
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
        return dateTime == null ? null : dateTime.toDate();
    }

    /**
     * @param date the date to set
     */
    public void setDate(Date date) {
        setDateTime(new DateTime(date));
    }

    /**
     * @return the dateTime
     */
    public DateTime getDateTime() {
        return dateTime;
    }

    /**
     * @param dateTime the dateTime to set
     */
    public void setDateTime(DateTime dateTime) {
        this.dateTime = dateTime;
    }    

    /**
     * @param count the count to add
     */
    public void add(long count) {
        this.count += count;
    }


    @Override
    public String toString() {
        return getClass().getSimpleName()+"["+getName()+":"+getCount()+" date:"+dateTime+"]";
    }


}
