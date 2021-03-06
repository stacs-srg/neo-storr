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

import uk.ac.standrews.cs.neoStorr.types.JPO_FIELD;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to manage mappings between names and slot numbers in LXP.
 */
public class JPOMetaData extends LXPMetaData {

    private final Map<String, JPOField> jpo_fields = new HashMap<>();

    public JPOMetaData(Class metadata_class, final String type_name) {

        super(metadata_class, type_name);
        while (metadata_class != null) {
            initialiseMaps(metadata_class);
            metadata_class = metadata_class.getSuperclass(); // run up hierarchy filling in fields
        }
    }

    private void initialiseMaps(final Class c) {

        final Field[] fields = c.getDeclaredFields();

        for (final Field field : fields) {

            field.setAccessible(true);

            if (!Modifier.isStatic(field.getModifiers()) && field.isAnnotationPresent(JPO_FIELD.class)) {

                String name = field.getName();
                Class type = field.getType();

                boolean is_list = List.class.isAssignableFrom(type);
                boolean lxp_ref = type.equals(LXPReference.class);
                boolean jpo_ref = type.equals(LXPReference.class);

                jpo_fields.put(name, new JPOField(name, type, is_list, lxp_ref, jpo_ref));
            }
        }
    }

    public Collection<JPOField> getStorrFields() {
        return jpo_fields.values();
    }

    public JPOField get(String key) {
        return jpo_fields.get(key);
    }

    public Class getMetadataClass() {
        return metadata_class;
    }
}
