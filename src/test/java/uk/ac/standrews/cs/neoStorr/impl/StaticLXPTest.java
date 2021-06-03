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


import org.junit.Before;
import org.junit.Test;
import uk.ac.standrews.cs.neoStorr.impl.exceptions.BucketException;
import uk.ac.standrews.cs.neoStorr.impl.exceptions.IllegalKeyException;
import uk.ac.standrews.cs.neoStorr.impl.exceptions.RepositoryException;
import uk.ac.standrews.cs.neoStorr.impl.transaction.exceptions.TransactionFailedException;
import uk.ac.standrews.cs.neoStorr.impl.transaction.interfaces.ITransaction;
import uk.ac.standrews.cs.neoStorr.interfaces.IBucket;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class StaticLXPTest extends CommonTest {

    private static String births_bucket_name = "Births";
    private IBucket<BBB> b;
    private BBB al;
    private long al_id;

    @Before
    public void setUp() throws RepositoryException, IOException, URISyntaxException, BucketException {

        System.out.println( "Running setUp()" );
        super.setUp();

        try {
            b = repository.getBucket(births_bucket_name, BBB.class);
            System.out.println( "Got bucket: " + births_bucket_name);
        } catch (RepositoryException e) {
            System.out.println( "Got exception getting bucket: " + e.getMessage() );
            System.out.println( "Trying to create bucket: " + births_bucket_name);
            b = repository.makeBucket(births_bucket_name, BBB.class);
            System.out.println( "Bucket: " + births_bucket_name + " created" );
        }
    }

    @Test
    public synchronized void testStaticLXPCreation() throws BucketException {
        al = new BBB();
        al.put( BBB.FORENAME,"Al" );
        al.put( BBB.SURNAME,"Dearle" );
        b.makePersistent( al );
        al_id = al.getId();
        assertTrue( b.contains( al_id ) );
    }

    @Test
    public synchronized void testStaticLXPCreateDelete() throws BucketException {
        al = new BBB();
        al.put( BBB.FORENAME,"Al" );
        al.put( BBB.SURNAME,"Dearle" );
        b.makePersistent( al );
        al_id = al.getId();
        assertTrue( b.contains( al_id ) );
        b.delete(al_id);
        assertFalse( b.contains( al_id ) );
    }

    @Test
    public synchronized void testStaticUpdate() throws RepositoryException, IllegalKeyException, BucketException, TransactionFailedException {

        al = new BBB();
        al.put( BBB.FORENAME,"Al" );
        al.put( BBB.SURNAME,"Dearle" );
        b.makePersistent( al );
        al_id = al.getId();

        ITransaction t = store.getTransactionManager().beginTransaction();
        al.put( BBB.FORENAME,"Alan");
        b.update(al);
        t.commit();
        assertEquals( al.get(BBB.FORENAME),"Alan" );
        //---------
        ITransaction t2 = store.getTransactionManager().beginTransaction();
        al.put( BBB.FORENAME,"Al");
        b.update(al);
        t2.commit();
        assertEquals( al.get(BBB.FORENAME),"Al" );
    }

    @Test
    public synchronized void testStaticAbort() throws RepositoryException, IllegalKeyException, BucketException, TransactionFailedException {

        al = new BBB();
        al.put( BBB.FORENAME,"Al" );
        al.put( BBB.SURNAME,"Dearle" );
        b.makePersistent( al );

        ITransaction t = store.getTransactionManager().beginTransaction();
        al.put( BBB.FORENAME,"Graham");
        b.update(al);
        t.rollback();
        assertEquals( al.get(BBB.FORENAME),"Al" );
    }



}
