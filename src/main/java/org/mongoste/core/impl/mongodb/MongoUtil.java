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
package org.mongoste.core.impl.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various MongoDB utility methods
 * @author mrmx
 */
public final class MongoUtil {
    public static final String DOT_STR                = ".";

    private static final String JS_FN_ADD = "db.system.js.save( { _id : \"%s\" , value : %s } );";
    private static final String JS_FN_DEL = "db.system.js.remove( {_id: \"%s\"} );";

    private static Logger log = LoggerFactory.getLogger(MongoUtil.class);

    public static void dropCollections(DB db) {
        for (String collection : db.getCollectionNames()) {
            if (collection.startsWith("system")) {
                continue;
            }
            log.warn("Dropping collection {}.{}", db.getName(),collection);
            DBCollection col = db.getCollection(collection);
            col.dropIndexes();
            col.drop();
        }
    }

    public static void addDBFunction(DB db,String functionName,String body) {
        log.info("Adding function {}.{} Function body:{}\n",new Object[]{db.getName(),functionName,body});
        db.eval(String.format(JS_FN_DEL, functionName));
        db.eval(String.format(JS_FN_ADD, functionName,body));
    }

    public static String createDotPath(Object... items) {
        StringBuilder sb = new StringBuilder();
        boolean hasNext = false;
        for(Object item : items ) {
            if(hasNext) {
                sb.append(DOT_STR);
            }
            sb.append(String.valueOf(item));
            hasNext = true;
        }
        return sb.toString();
    }

    public static DBObject createDoc(Object... values) {
        if (values != null && values.length % 2 != 0) {
            throw new IllegalArgumentException("odd value array size");
        }
        DBObject doc = new BasicDBObject();
        if (values != null) {
            int size = values.length;
            for (int i = 0; i < size; i += 2) {
                doc.put(String.valueOf(values[i]), values[i + 1]);
            }
        }
        return doc;
    }

    public static BasicDBObject getChildDBObject(BasicDBObject source, String dotPath) {
        return getChildDBObject(source, dotPath, Integer.MAX_VALUE);
    }

    public static BasicDBObject getChildDBObject(BasicDBObject source, String dotPath,int maxDepth) {
        String  [] path = dotPath.split("\\.");
        BasicDBObject result = source;
        for(String key : path) {
            if(result == null) {
                break;
            }
            result = (BasicDBObject) result.get(key);
            if(--maxDepth == 0) {
                break;
            }
        }
        return result;
    }

    public static void close(DBCursor dbc) {
        //TODO incoming driver version method:
        //dbc.close();
    }
}
