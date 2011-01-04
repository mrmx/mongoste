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

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a stat action counter
 * @author mrmx
 */
public class StatAction extends StatBasicCounter {
    private String name;
    private long count;
    private List<StatBasicCounter> targets;

    public StatAction(String name, long count) {
        super(name,count);
        this.targets = new ArrayList<StatBasicCounter>();
    }

    /**
     * @return the targets
     */
    public List<StatBasicCounter> getTargets() {
        return targets;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()+"["+getName()+":"+getCount()+",targets:"+getTargets()+"]";
    }
    
}