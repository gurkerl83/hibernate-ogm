/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.hibernate.MappingException;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.util.MarkerObject;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Subclass;
import org.hibernate.sql.InFragment;
import org.hibernate.type.DiscriminatorType;
import org.hibernate.type.StringType;
import org.hibernate.type.Type;

/**
 * The discriminator is a column containing a different value for each entity type.
 *
 * @author "Davide D'Alto"
 */
class ColumnBasedDiscriminator implements EntityDiscriminator {

	private static final Object NULL_DISCRIMINATOR = new MarkerObject( "<null discriminator>" );
	private static final Object NOT_NULL_DISCRIMINATOR = new MarkerObject( "<not null discriminator>" );

	private final String alias;
	private final String columnName;
	private final Type discriminatorType;
	private final boolean forced;
	private final boolean needed;
	private final String sqlValue;
	private final Object value;
	private final Map<Object, String> subclassByValue;

	public ColumnBasedDiscriminator(final PersistentClass persistentClass, final SessionFactoryImplementor factory, final Column column) {
		forced = persistentClass.isForceDiscriminator();
		columnName = column.getQuotedName( factory.getDialect() );
		alias = column.getAlias( factory.getDialect(), persistentClass.getRootTable() );
		discriminatorType = persistentClass.getDiscriminator().getType() == null
				? StringType.INSTANCE
				: persistentClass.getDiscriminator().getType();
		value = value( persistentClass, discriminatorType );
		sqlValue = sqlValue( persistentClass, factory.getDialect(), value, discriminatorType );
		subclassByValue = subclassesByValue( persistentClass, value, discriminatorType );
		needed = true;
	}

	private static Map<Object, String> subclassesByValue(final PersistentClass persistentClass, Object value, Type type) {
		Map<Object, String> subclassesByDsicriminator = new HashMap<Object, String>();
		subclassesByDsicriminator.put( value, persistentClass.getEntityName() );

		if ( persistentClass.isPolymorphic() ) {
			@SuppressWarnings("unchecked")
			Iterator<Subclass> iter = persistentClass.getSubclassIterator();
			while ( iter.hasNext() ) {
				Subclass sc = iter.next();
				subclassesByDsicriminator.put( value( sc, type ), sc.getEntityName() );
			}
		}
		return subclassesByDsicriminator;
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

	@Override
	public String provideClassByValue(Object value) {
		return subclassByValue.get( value );
	}

	@Override
	public String getSqlValue() {
		return sqlValue;
	}

	@Override
	public String getColumnName() {
		return columnName;
	}

	@Override
	public String getAlias() {
		return alias;
	}

	@Override
	public Type getType() {
		return discriminatorType;
	}

	@Override
	public Object getValue() {
		return value;
	}

	@Override
	public boolean isForced() {
		return forced;
	}

	@Override
	public boolean isNeeded() {
		return needed;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append( "ColumnBasedDiscriminator [alias=" );
		builder.append( alias );
		builder.append( ", columnName=" );
		builder.append( columnName );
		builder.append( ", discriminatorType=" );
		builder.append( discriminatorType );
		builder.append( ", forced=" );
		builder.append( forced );
		builder.append( ", needed=" );
		builder.append( needed );
		builder.append( ", sqlValue=" );
		builder.append( sqlValue );
		builder.append( ", value=" );
		builder.append( value );
		builder.append( ", subclassByValue=" );
		builder.append( subclassByValue );
		builder.append( "]" );
		return builder.toString();
	}

}
