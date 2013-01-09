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

import static org.hibernate.ogm.persister.DiscriminatorHelper.sqlValue;
import static org.hibernate.ogm.persister.DiscriminatorHelper.value;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.type.StringType;
import org.hibernate.type.Type;

/**
 * The discriminator is a column.
 *
 * @author "Davide D'Alto"
 */
class ColumnDiscriminator {

	static final ColumnDiscriminator NOT_NEEDED = new ColumnDiscriminator();

	private final String alias;
	private final String columnName;
	private final Type discriminatorType;
	private final boolean forced;
	private final String sqlValue;
	private final Object value;

	private ColumnDiscriminator() {
		alias = null;
		columnName = null;
		discriminatorType = null;
		forced = false;
		sqlValue =null;
		value = null;
	}

	public ColumnDiscriminator(final PersistentClass persistentClass, final SessionFactoryImplementor factory,
			final Column column) {
		forced = persistentClass.isForceDiscriminator();
		columnName = column.getQuotedName( factory.getDialect() );
		alias = column.getAlias( factory.getDialect(), persistentClass.getRootTable() );
		discriminatorType = persistentClass.getDiscriminator().getType() == null
				? StringType.INSTANCE
				: persistentClass.getDiscriminator().getType();
		value = value( persistentClass, discriminatorType );
		sqlValue = sqlValue( persistentClass, factory.getDialect(), value, discriminatorType );
	}

	public String getSqlValue() {
		return sqlValue;
	}

	public String getColumnName() {
		return columnName;
	}

	public String getAlias() {
		return alias;
	}

	public Type getType() {
		return discriminatorType;
	}

	public Object getValue() {
		return value;
	}

	public boolean isForced() {
		return forced;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append( "SingleTableDiscriminator [alias=" );
		builder.append( alias );
		builder.append( ", columnName=" );
		builder.append( columnName );
		builder.append( ", discriminatorType=" );
		builder.append( discriminatorType );
		builder.append( ", forced=" );
		builder.append( forced );
		builder.append( ", sqlValue=" );
		builder.append( sqlValue );
		builder.append( ", value=" );
		builder.append( value );
		builder.append( "]" );
		return builder.toString();
	}

}
