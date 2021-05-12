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
package uk.ac.standrews.cs.storr.impl.transaction.impl;

import uk.ac.standrews.cs.storr.impl.exceptions.RepositoryException;
import uk.ac.standrews.cs.storr.impl.transaction.exceptions.TransactionFailedException;
import uk.ac.standrews.cs.storr.impl.transaction.interfaces.ITransaction;
import uk.ac.standrews.cs.storr.impl.transaction.interfaces.ITransactionManager;
import uk.ac.standrews.cs.storr.interfaces.IStore;
import uk.ac.standrews.cs.storr.util.NeoDbCypherBridge;

import java.util.HashMap;

/**
 * Created by al on 05/01/15.
 */
public class TransactionManager implements ITransactionManager {

    private final IStore store;
    private HashMap<String, ITransaction> map = new HashMap<>();


    public TransactionManager(final IStore store) throws RepositoryException {

        this.store = store;
    }

    @Override
    public ITransaction beginTransaction() throws TransactionFailedException {

        final Transaction t = new Transaction(this);
        map.put(t.getId(), t);
        return t;
    }

    @Override
    public ITransaction getTransaction(final String id) {
        return map.get(id);
    }

    @Override
    public void removeTransaction(final ITransaction t) {

        if (!t.isActive()) {
            map.remove(t.getId());
        }
    }

    public NeoDbCypherBridge getBridge() { return store.getBridge(); }
}
