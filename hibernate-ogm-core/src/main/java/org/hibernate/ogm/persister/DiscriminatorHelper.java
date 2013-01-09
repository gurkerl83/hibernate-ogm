/* 
 * Hibernate, Relational Persistence for Idiomatic Java
 * 
 * JBoss, Home of Professional Open Source
 * Copyright ${year} Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.hibernate.ogm.persister;

import org.hibernate.MappingException;
import org.hibernate.dialect.Dialect;
import org.hibernate.internal.util.MarkerObject;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.sql.InFragment;
import org.hibernate.type.DiscriminatorType;
import org.hibernate.type.Type;

/**
 * Utility class containing methods to use when dealing with discriminators.
 *
 * @author "Davide D'Alto"
 */
class DiscriminatorHelper {

	private static final Object NULL_DISCRIMINATOR = new MarkerObject( "<null discriminator>" );
	private static final Object NOT_NULL_DISCRIMINATOR = new MarkerObject( "<not null discriminator>" );

	public DiscriminatorHelper() {
	}

	public static String sqlValue(PersistentClass persistentClass, Dialect dialect,
			Object value, Type discriminatorType) {
		try {
			return obtainSqlValue( persistentClass, dialect, value, discriminatorType );
		}
		catch ( ClassCastException cce ) {
			throw new MappingException( "Illegal discriminator type: " + discriminatorType.getName() );
		}
		catch ( Exception e ) {
			throw new MappingException( "Could not format discriminator value to SQL string", e );
		}
	}

	private static String obtainSqlValue(PersistentClass persistentClass, Dialect dialect,
			Object value, Type discriminatorType) throws Exception {
		if ( persistentClass.isDiscriminatorValueNull() ) {
			return InFragment.NULL;
		}

		if ( persistentClass.isDiscriminatorValueNotNull() ) {
			return InFragment.NOT_NULL;
		}

		@SuppressWarnings("unchecked")
		DiscriminatorType<Object> dtype = (DiscriminatorType<Object>) discriminatorType;
		return dtype.objectToSQLString( value, dialect );
	}

	public static Object value(PersistentClass persistentClass, Type discriminatorType) {
		try {
			return obtainValue( persistentClass, discriminatorType );
		}
		catch ( ClassCastException cce ) {
			throw new MappingException( "Illegal discriminator type: " + discriminatorType.getName() );
		}
		catch ( Exception e ) {
			throw new MappingException( "Could not convert string to discriminator object", e );
		}
	}

	private static Object obtainValue(PersistentClass persistentClass, Type discriminatorType)
			throws Exception {
		if ( persistentClass.isDiscriminatorValueNull() ) {
			return NULL_DISCRIMINATOR;
		}

		if ( persistentClass.isDiscriminatorValueNotNull() ) {
			return NOT_NULL_DISCRIMINATOR;
		}

		@SuppressWarnings("unchecked")
		DiscriminatorType<Object> dtype = (DiscriminatorType<Object>) discriminatorType;
		return dtype.stringToObject( persistentClass.getDiscriminatorValue() );
	}

}
