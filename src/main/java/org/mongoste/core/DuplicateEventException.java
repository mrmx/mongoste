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
 * Duplicate event stats engine exception
 * @author mrmx
 */
public class DuplicateEventException extends StatsEngineException {

	/**
	 * Serial version
	 */
	private static final long serialVersionUID = -8318719826740661005L;

	public DuplicateEventException() {
    }

    public DuplicateEventException(String msg) {
        super(msg);
    }

    public DuplicateEventException(String msg, Throwable cause) {
        super(msg,cause);
    }

}
