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
package uk.ac.standrews.cs.neoStorr.impl;

import org.junit.After;
import org.junit.Before;
import uk.ac.standrews.cs.neoStorr.impl.exceptions.RepositoryException;
import uk.ac.standrews.cs.neoStorr.interfaces.IRepository;
import uk.ac.standrews.cs.neoStorr.interfaces.IStore;

import static org.junit.Assert.assertFalse;

public abstract class CommonTest {

    static final String REPOSITORY_NAME = "REPO_3465987345";
    static final String BUCKET_NAME = "BUCKET_324895733346";

    protected IStore store;
    protected IRepository repository;

    public static void main(String[] args) throws RepositoryException {

        // Run this to delete existing repository.
        IStore store = Store.getInstance();
        if (store.repositoryExists(REPOSITORY_NAME)) {
            store.deleteRepository(REPOSITORY_NAME);
        }
        System.exit(0);
    }

    @Before
    public void setUp() throws Exception {

        store = Store.getInstance();

        // Clean up in case of any previous incomplete cleaning.
        if (store.repositoryExists(REPOSITORY_NAME)) store.deleteRepository(REPOSITORY_NAME);

        repository = store.makeRepository(REPOSITORY_NAME);
        repository.makeBucket(BUCKET_NAME);
    }

    @After
    public void tearDown() throws RepositoryException {

        repository.deleteBucket(BUCKET_NAME);
        store.deleteRepository(REPOSITORY_NAME);

        assertFalse(store.repositoryExists(REPOSITORY_NAME));
    }
}
