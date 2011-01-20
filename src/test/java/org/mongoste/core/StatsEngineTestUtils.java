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


import org.mongoste.model.StatEvent;
import org.mongoste.util.DateUtil;

import java.util.Calendar;

/**
 * Some test utils
 * @author mrmx
 */
public class StatsEngineTestUtils {

    public static void buildSamples(StatsEngine statsEngine) throws StatsEngineException {
        StatEvent event;
        Calendar cal = DateUtil.getCalendarGMT0();
        DateUtil.trimTime(cal);
        cal.set(Calendar.DATE,1);
        //cal.set(Calendar.MONTH,C);
        cal.set(Calendar.HOUR_OF_DAY,0);
        int metaCount = 0;
        for(int j = 0; j < 4 ; j++) {
            for(int i = 0; i < 10 ; i++) {
                event = new StatEvent();
                event.setClientId("client1");
                //cal.set(Calendar.HOUR,i);
                //cal.set(Calendar.HOUR,i % 11);
                event.setDate(cal.getTime());
                //event.setDate(DateUtil.getDateGMT0());
                event.setTarget("target"+(1+j%2));
                event.setAction("view");
                event.setTargetType("work");
                //event.getMetadata().put("sessionId", j * 100 + metaCount );
                event.getMetadata().put("ip", "192.168.1." + ((metaCount) % 10) );
                statsEngine.handleEvent(event);
                metaCount = metaCount + (i % 2);
                cal.add(Calendar.HOUR_OF_DAY,i % 4);
            }
            cal.add(Calendar.DATE,1);
        }
    }
}