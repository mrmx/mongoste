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

package org.mongoste.query;

import java.util.Arrays;

/**
 * Exception for some required field/s
 * @author mrmx
 */
public class SomeRequiredQueryFieldException extends RequiredQueryFieldException {

    public SomeRequiredQueryFieldException(QueryField ... fields) {
        super("some of "+Arrays.asList(fields) + " field/s is/are required");
    }

}
