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
package uk.ac.standrews.cs.storr.impl;

import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;
import uk.ac.standrews.cs.storr.impl.exceptions.RepositoryException;
import uk.ac.standrews.cs.storr.impl.exceptions.StoreException;
import uk.ac.standrews.cs.storr.interfaces.IRepository;
import uk.ac.standrews.cs.storr.interfaces.IStore;
import uk.ac.standrews.cs.storr.util.NeoDbCypherBridge;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.neo4j.driver.Values.parameters;
import static uk.ac.standrews.cs.storr.impl.Repository.repositoryNameIsLegal;

/**
 * Created by al on 06/06/2014.
 */
public class Store implements IStore {

    private static Store instance;
    private final TypeFactory type_factory;
    private final Map<String, IRepository> repository_cache;

    public NeoDbCypherBridge getBridge() {
        return bridge;
    }

    private final NeoDbCypherBridge bridge;

    private static final String CREATE_REPO = "MERGE (a:STORR_REPOSITORY {name: $name})";
    private static final String REPO_EXISTS = "MATCH (r:STORR_REPOSITORY {name: $name}) return r";

    public Store() throws StoreException {

        instance = this;

        try {
            bridge = new NeoDbCypherBridge();
            repository_cache = new HashMap<>();

            type_factory = new TypeFactory(this);

        } catch ( RepositoryException e) {
            throw new StoreException(e.getMessage());
        }
    }

    public synchronized static IStore getInstance() {
        return instance;
    }

    @Override
    public TypeFactory getTypeFactory() {
        return type_factory;
    }

    @Override
    public IRepository makeRepository(final String name) throws RepositoryException {

        if (!repositoryNameIsLegal(name)) {
            throw new RepositoryException("Illegal Repository name <" + name + ">");
        }
        createRepository(name);
        IRepository r = getRepository(name);
        repository_cache.put(name, r);
        return r;
    }

    @Override
    public boolean repositoryExists(String name) {
        Result result = bridge.getNewSession().run(REPO_EXISTS,parameters("name", name));
        List<Node> nodes = result.list(r -> r.get("r").asNode());
        if( nodes.size() == 0 ) {
            return false;
        }
        return true;
    }

    @Override
    public void deleteRepository(String name) throws RepositoryException {
        if (!repositoryExists(name)) {
            throw new RepositoryException("Bucket with " + name + "does not exist");
        }

    }

    ////////////////// private and protected methods //////////////////

    @Override
    public IRepository getRepository(String name) throws RepositoryException {

        if (repositoryExists(name)) {
            if (repository_cache.containsKey(name)) {
                return repository_cache.get(name);
            } else {
                IRepository r = new Repository(this, name);
                repository_cache.put(name, r);
                return r;
            }
        }
        throw new RepositoryException("repository does not exist: " + name);
    }

    @Override
    public Iterator<IRepository> getIterator() {
        return null; }

//    private Path getRepoPath(final String name) {
//
//        return repository_path.resolve(name);
//    }

    private void createRepository(String name) throws RepositoryException {

        if (repositoryExists(name)) {
            throw new RepositoryException("Repo: " + name + " already exists" );
        }

        try ( Session session = bridge.getNewSession() )
        {
            session.writeTransaction(tx -> tx.run(CREATE_REPO, parameters("name", name)));
        }
    }
}