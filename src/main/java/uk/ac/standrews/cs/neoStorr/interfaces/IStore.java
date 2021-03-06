/*
 * Copyright 2021 Systems Research Group, University of St Andrews:
 * <https://github.com/stacs-srg>
 *
 * This file is part of the module neo-storr.
 *
 * neo-storr is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * neo-storr is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with neo-storr. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package uk.ac.standrews.cs.neoStorr.interfaces;

import uk.ac.standrews.cs.neoStorr.impl.TypeFactory;
import uk.ac.standrews.cs.neoStorr.impl.exceptions.RepositoryException;
import uk.ac.standrews.cs.neoStorr.impl.transaction.interfaces.ITransactionManager;
import uk.ac.standrews.cs.neoStorr.util.NeoDbCypherBridge;

/**
 * This interface is used to encode the type of a Store within the system.
 * The Stores are implemented as a Hierarchy in which there is one instance of the Store (per node).
 * Each Store can contain a multiplicity of Repositories (implementing @class IRepository).
 * Each repository can contain buckets (implementing at least @class IBucket).
 * Buckets contain OID (labelled cross product) records.
 * Repositories (Repos) and buckets may be deleted.
 * The reason this model was adopted was to be permit buckets to be clustered together into local groups.
 * For example, buckets of blocked records may be clustered together into a single repo.
 * Another example is the types repo which contains 2 different buckets used to represent different aspects of the type system.
 * Created by al on 06/06/2014.
 */
public interface IStore {

    /**
     * @return the transaction manager associated with the store
     */
    ITransactionManager getTransactionManager();

    TypeFactory getTypeFactory();

    /**
     * @param name - the name of the repository to be created
     * @return a new repository with the given name
     * @throws RepositoryException - if the repo exists already of if something goes wrong.
     */
    IRepository makeRepository(String name) throws RepositoryException;

    /**
     * @param name - the repo that is the subject of the enquiry.
     * @return true if a repository with the given name exists in the store.
     */
    boolean repositoryExists(String name);

    /**
     * This method deletes the specified repository
     *
     * @param name - the name of the repo to be deleted.
     * @throws RepositoryException - if the repo does not exist or something goes wrong
     */
    void deleteRepository(String name) throws RepositoryException;

    /**
     * @param name - the name of the repo being looked up
     * @return the repo with the given name, if it exists.
     * @throws RepositoryException if the repo does not exist or if something goes wrong.
     */
    IRepository getRepository(String name) throws RepositoryException;

    /**
     * @return the neo4J bridge created by the store
     */
    NeoDbCypherBridge getBridge();

    /**
     * close the store
     */
    void close();
}
