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

import uk.ac.standrews.cs.neoStorr.impl.PersistentObject;
import uk.ac.standrews.cs.neoStorr.impl.exceptions.BucketException;

import java.util.List;

/**
 * The interface for a Bucket (a repository of OID records).
 * Each record in the repository is identified by id.
 * <p>
 * Operations in this class mirror those in JDO:
 * T getObjectById(long id) throws BucketException;
 * void makePersistent(T record) throws BucketException;
 */
public interface IBucket<T extends PersistentObject> {

    /**
     * Gets the OID record with the specified id
     *
     * @param id - the identifier of the OID record for which a reader is required.
     * @return an OID record with the specified id, or null if the record cannot be found
     * @throws BucketException if the record cannot be found or if something goes wrong.
     */
    T getObjectById(long id) throws BucketException;

    /**
     * Synchronously writes the state of a record to a bucket.
     * The id of the record is used to determine its name in the bucket.
     * When this operation returns data is stored resiliently.
     *
     * @param record whose state is to be written.
     * @throws BucketException if an error occurs during the operation.
     */
    void makePersistent(T record) throws BucketException;

    /**
     * Updates the state of the specified record in the store.
     * Must be performed in the context of a transaction
     *
     * @param record the record to be updated
     * @throws BucketException if an error occurs during the operation.
     */
    void update(T record) throws BucketException;

    /**
     * Delete the record with the specified oid
     *
     * @param oid denoting the record to be deleted
     * @throws BucketException if an error occurs during the operation.
     */
    void delete(long oid) throws BucketException;

    /**
     * @param cache_size - set the size of the object cache being implemented by the bucket
     * @throws BucketException if the cache size if smaller than the currently set cache size (i.e. cannot lose cached information
     */
    void setCacheSize(int cache_size) throws BucketException;

    /**
     * @return the size of the object cache being implemented by the bucket
     */
    int getCacheSize();

    /**
     * @return an input Stream containing all the OID records in this Bucket
     * @throws BucketException if an error occurs during the operation.
     */
    IInputStream<T> getInputStream() throws BucketException;

    /**
     * @return an output Stream which supports the writing of records to this Bucket
     */
    IOutputStream<T> getOutputStream();

    /**
     * @return the oids of the records that are in this bucket
     */
    List<Long> getObjectIds();

    /**
     * @return the name of the bucket
     */
    String getName();

    /**
     * @return the repository in which the bucket is located
     */
    IRepository getRepository();

    /**
     * Returns the number of records stored in the bucket
     */
    int size() throws BucketException;

    /**
     * A predicate to determine if a OID with the given id is located in the bucket.
     *
     * @param id - an id to lookup
     * @return true if the bucket contains the given id
     */
    boolean contains(long id);

    /**
     * @return the class associated with the bucket if there is one and null if there is not.
     */
    Class<T> getBucketType();

    /**
     * Sets the type of the bucket contents.
     */
    void setPersistentTypeLabelID() throws BucketException;

    /**
     * Used to invalidate cached information when updates to underlying data structures are updated
     */
    void invalidateCache();
}
