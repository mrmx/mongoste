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

/**
 * Base class for all engines
 * @author mrmx
 */
public abstract class AbstractStatsEngine implements StatsEngine {
    static final String DOT_STR                = ".";
    static final char DOT_CHR                  = '.';
    static final char DOT_CHR_REPLACE          = '_';

    private boolean keepEvents = true;

    @Override
    public void setKeepEvents(boolean keepEvents) {
        this.keepEvents = keepEvents;
    }

    @Override
    public boolean isKeepEvents() {
        return keepEvents;
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