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

import uk.ac.standrews.cs.neoStorr.impl.LXP;
import uk.ac.standrews.cs.neoStorr.impl.exceptions.BucketException;
import uk.ac.standrews.cs.neoStorr.impl.exceptions.RepositoryException;

import java.util.Iterator;

/**
 * Classes implementing this interface is used to represent repositories.
 * See further comments in @class IStore
 * Created by al on 11/05/2014.
 */
public interface IRepository {

    /**
     * This method creates a new bucket
     *
     * @param name - the name of the bucket to be created.
     * @return the newly created repository
     * @throws RepositoryException if a bucket with the name previously exists or if something goes wrong.
     */
    IBucket makeBucket(String name) throws RepositoryException;

    /**
     * This method creates a new bucket that is constrained to contain OID records compatible with T.
     *
     * @param name     - the name of the bucket to be created.
     * @param bucketType - the type being used to create instances in this bucket
     * @param <T>      the (Java) type which all LXP derived objects in this bucket are expected to be of
     * @return the newly created repository
     * @throws RepositoryException if a bucket with the name previously exists or if something goes wrong.
     */
    <T extends LXP> IBucket<T> makeBucket(final String name, Class<T> bucketType) throws RepositoryException, BucketException;

    /**
     * @param name - the bucket that is the subject of the enquiry.
     * @return true if a bucket with the given name exists in the repo.
     */
    boolean bucketExists(String name);

    /**
     * This method deletes the specified bucket
     *
     * @param name - the name of the bucket to be deleted.
     * @throws RepositoryException - if the bucket does not exist or something goes wrong
     */
    void deleteBucket(String name) throws RepositoryException;

    /**
     * @param name - the name of the bucket being looked up
     * @return the bucket with the given name, if it exists.
     * @throws RepositoryException if the bucket does not exist or if something goes wrong.
     */
    IBucket getBucket(final String name) throws RepositoryException;

    /**
     * @param name     - the name of the bucket being looked up
     * @param bucketType - a class capable of creating instances of type @class T
     * @param <T>      the (Java) type which all LXP derived objects in this bucket are expected to be of
     * @return the bucket with the given name, if it exists and is type compatible
     * @throws RepositoryException if the bucket does not exist or if something goes wrong.
     */
    <T extends LXP> IBucket<T> getBucket(final String name, Class<T> bucketType) throws RepositoryException;

    /**
     * @return the names of all the buckets in the repo
     * Note this returns strings and not buckets since they may be of different types
     * (i.e. either constrained by type (homogeneous) or not (heterogeneous).
     */
    Iterator<String> getBucketNameIterator();

    /**
     * Returns an iterator over those buckets that are constrained by T.
     * @param bucketType - specifies the type constraining the bucket
     * @param <T>      - the type of the required bucket content types.
     * @return an iterator over the appropriate buckets.
     */
    <T extends LXP> Iterator<IBucket<T>> getIterator(Class<T> bucketType);

    /**
     * @return the name of the repository
     */
    String getName();

    /**
     * @return the store which contains the repository
     */
    IStore getStore();
}
