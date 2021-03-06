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
package uk.ac.standrews.cs.neoStorr.impl.transaction.impl;

import uk.ac.standrews.cs.neoStorr.impl.exceptions.RepositoryException;
import uk.ac.standrews.cs.neoStorr.impl.transaction.interfaces.ITransaction;
import uk.ac.standrews.cs.neoStorr.impl.transaction.interfaces.ITransactionManager;
import uk.ac.standrews.cs.neoStorr.interfaces.IStore;
import uk.ac.standrews.cs.neoStorr.util.NeoDbCypherBridge;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by al on 05/01/15.
 */
public class TransactionManager implements ITransactionManager {

    private final IStore store;
    private final Map<String, ITransaction> map = Collections.synchronizedMap(new HashMap<>());

    private boolean auto_commit = true;

    public TransactionManager(final IStore store) throws RepositoryException {
        this.store = store;
    }

    @Override
    public ITransaction beginTransaction() {

        final Transaction t = new Transaction(this);
        map.put(t.getId(), t);
        return t;
    }

    @Override
    public ITransaction getTransaction(final String id) {
        return map.get(id);
    }

    public void setAutoCommit(final boolean auto_commit) {
        this.auto_commit = auto_commit;
    }

    public boolean isAutoCommitEnabled() {
        return auto_commit;
    }

    public NeoDbCypherBridge getBridge() { return store.getBridge(); }
}
