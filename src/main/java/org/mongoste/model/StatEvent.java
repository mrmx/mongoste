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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
    private Date   date;
    private Calendar calendar;
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
        return date;
    }

    /**
     * @param date the date to set
     */
    public void setDate(Date date) {
        this.calendar = null;
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


    public int getYear() {
        return getCalendar().get(Calendar.YEAR);
    }


    public int getWeek() {
        return getCalendar().get(Calendar.WEEK_OF_YEAR);
    }

    public int getMonth() {
        return getCalendar().get(Calendar.MONTH) + 1;
    }

    public int getDay() {
        return getCalendar().get(Calendar.DATE);
    }

    public int getHour() {
        return getCalendar().get(Calendar.HOUR_OF_DAY);
    }

    private Calendar getCalendar()  {
        if(calendar == null) {
            calendar = Calendar.getInstance();
            calendar.setTime(getDate());
        }
        return calendar;
    }

}
