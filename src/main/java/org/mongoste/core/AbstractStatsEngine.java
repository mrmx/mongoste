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
package org.mongoste.core;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Base class for all engines
 * @author mrmx
 */
public abstract class AbstractStatsEngine implements StatsEngine {
    static final String DOT_STR                = ".";
    static final char DOT_CHR                  = '.';
    static final char DOT_CHR_REPLACE          = '_';

    private TimeScope timeScopePrecision;

    @Override
    public void setTargetOwners(String clientId, String targetType, String target, List<String> owners) throws StatsEngineException {
        setTargetOwners(clientId, targetType, Arrays.asList(target), owners);
    }

    public void setTimeScopePrecision(String precision) throws StatsEngineException {
        TimeScope timeScope = null;
        try {
            timeScope = TimeScope.valueOf(precision.trim().toUpperCase());
        }catch(Exception ex) {
            throw new StatsEngineException("Invalid time scope precision: "+precision, ex);
        }
        setTimeScopePrecision(timeScope);
    }

    @Override
    public void setTimeScopePrecision(TimeScope precision) throws StatsEngineException {
        if(precision == null) {
            throw new IllegalArgumentException("null precision");
        }
        List<TimeScope> supported = getSupportedTimeScopePrecision();
        if(supported == null || supported.isEmpty()) {
            throw new StatsEngineException("engine has not defined supported precision list");
        }
        if(!supported.contains(precision)) {
            throw new StatsEngineException("Unsupported precision "+precision);
        }
        this.timeScopePrecision = precision;
    }

    @Override
    public TimeScope getTimeScopePrecision() {
        return timeScopePrecision;
    }
    
    /**
     * Dot notation builder method
     * @param items
     * @return dot path
     */
    protected final String createDotPath(Object... items) {
        StringBuilder sb = new StringBuilder();
        boolean hasNext = false;
        String value;
        for(Object item : items ) {            
            if(item == null
               || StringUtils.isBlank(value = String.valueOf(item))
               || DOT_STR.equals(value.trim())
              ){
                continue;
            }
            while(value.endsWith(DOT_STR)) {
                value = value.substring(0,value.length()-1);
            }
            if(hasNext) {
                sb.append(DOT_STR);
            }
            //value = value.replace(DOT_CHR, DOT_CHR_REPLACE);
            sb.append(value);
            hasNext = true;
        }
        return sb.toString();
    }

}