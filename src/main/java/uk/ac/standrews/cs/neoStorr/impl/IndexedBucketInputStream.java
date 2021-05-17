/*
 * Copyright 2017 Systems Research Group, University of St Andrews:
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

import uk.ac.standrews.cs.neoStorr.impl.exceptions.BucketException;
import uk.ac.standrews.cs.neoStorr.interfaces.IBucket;
import uk.ac.standrews.cs.neoStorr.interfaces.IInputStream;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class IndexedBucketInputStream implements IInputStream {

    private final IBucket bucket;
    private Iterator<File> file_iterator;

    IndexedBucketInputStream(final IBucket bucket, final Iterator<File> file_iterator) throws IOException {

        this.bucket = bucket;
        this.file_iterator = file_iterator;
    }

    public Iterator<PersistentObject> iterator() {

        return new Iterator<PersistentObject>() {

            public boolean hasNext() {
                return file_iterator.hasNext();
            }

            @Override
            public PersistentObject next() {

                try {
                    return bucket.getObjectById(Long.parseLong(file_iterator.next().getName()));

                } catch (BucketException e) {
                    throw new NoSuchElementException(e.getMessage());
                }
            }
        };
    }
}
