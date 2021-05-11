/*
 * Copyright 2017 Systems Research Group, University of St Andrews:
 * <https://github.com/stacs-srg>
 *
 * This file is part of the module storr.
 *
 * storr is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * storr is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with storr. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package uk.ac.standrews.cs.storr.util;

public abstract class NeoDbBridge implements AutoCloseable {

    protected static final String default_url = "bolt://localhost:7687";
    protected static final String default_user = "neo4j";
    protected static final String default_password = "password";

    protected final String uri;
    protected final String user;
    protected final String password;

    public NeoDbBridge() {
        this( default_url,default_user,default_password );
    }

    public NeoDbBridge(String uri, String user, String password) {
        this.uri = uri;
        this.user = user;
        this.password = password;
    }

    public abstract void close() throws Exception;
}
