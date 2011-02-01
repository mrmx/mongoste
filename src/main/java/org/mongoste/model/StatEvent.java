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

import java.util.Calendar;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

import org.mongoste.util.DateUtil;

/**
 * Main stat event object
 * @author mrmx
 */
public class StatEvent {
    private String clientId;
    private String target;
    private String targetType;
    private List<String> targetOwners;
    private List<String> targetTags;
    private String action;
    private DateTime   date;    
    private Map<String,Object> metadata;

    /**
     * @return the clientId
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * @param clientId the clientId to set
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * @return the target
     */
    public String getTarget() {
        return target;
    }

    /**
     * @param target the target to set
     */
    public void setTarget(String target) {
        this.target = target;
    }

    /**
     * @return the targetType
     */
    public String getTargetType() {
        return targetType;
    }

    /**
     * @param targetType the targetType to set
     */
    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    /**
     * @return the targetOwners
     */
    public List<String> getTargetOwners() {
        return targetOwners;
    }

    /**
     * @param targetOwners the targetOwners to set
     */
    public void setTargetOwners(List<String> targetOwners) {
        this.targetOwners = targetOwners;
    }

    /**
     * @return the targetTags
     */
    public List<String> getTargetTags() {
        return targetTags;
    }

    /**
     * @param targetTags the targetTags to set
     */
    public void setTargetTags(List<String> targetTags) {
        this.targetTags = targetTags;
    }

    /**
     * @return the action
     */
    public String getAction() {
        return action;
    }

    /**
     * @param action the action to set
     */
    public void setAction(String action) {
        this.action = action;
    }

    /**
     * @return the date
     */
    public Date getDate() {
        return date.toDate();
    }

    /**
     * @param date the date to set
     */
    public void setDate(Date date) {        
        setDateTime(new DateTime(date));
    }

    /**
     * @param date the date to set
     */
    public void setDate(DateTime date) {
        setDateTime(date);
    }
    
    /**
     * @return the date
     */
    public DateTime getDateTime() {
        return date;
    }

    /**
     * @param date the date to set
     */
    public void setDateTime(DateTime date) {
        this.date = date;
    }

    /**
     * @return the metadata
     */
    public Map<String, Object> getMetadata() {
        if(metadata == null) {
            metadata = new TreeMap<String, Object>();
        }
        return metadata;
    }

    /**
     * @param metadata the metadata to set
     */
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public DateTime getYearMonthDate() {
        MutableDateTime dt = DateUtil.getDateTimeUTC().toMutableDateTime();
        dt.setDateTime(getYear(), getMonth(), 1 ,0, 0, 0, 0);
        return dt.toDateTime();
    }

    public int getYear() {
        return getDateTime().getYear();
    }

    public int getWeek() {
        return getDateTime().getWeekOfWeekyear();
    }

    public int getMonth() {
        return getDateTime().getMonthOfYear();
    }

    public int getDay() {
        return getDateTime().getDayOfMonth();
    }

    public int getHour() {
        return getDateTime().getHourOfDay();
    }

}
