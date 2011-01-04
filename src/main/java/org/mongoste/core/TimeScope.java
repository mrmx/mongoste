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

/**
 * Defines time scopes
 * @author mrmx
 */
public enum TimeScope {

    HOURLY("h"), DAILY("d"), WEEKLY("w"), MONTHLY("m"), ANNUAL("a"), GLOBAL("g");

    private String key;

    private TimeScope(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
