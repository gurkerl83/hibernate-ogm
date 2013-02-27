/* 
 * Hibernate, Relational Persistence for Idiomatic Java
 * 
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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
package org.hibernate.ogm.datastore.map.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.ScrollableResults;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.dialect.lock.OptimisticForceIncrementLockingStrategy;
import org.hibernate.dialect.lock.OptimisticLockingStrategy;
import org.hibernate.dialect.lock.PessimisticForceIncrementLockingStrategy;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.ogm.datastore.impl.EmptyTupleSnapshot;
import org.hibernate.ogm.datastore.impl.MapTupleSnapshot;
import org.hibernate.ogm.datastore.impl.MapHelpers;
import org.hibernate.ogm.datastore.spi.Association;
import org.hibernate.ogm.datastore.spi.AssociationContext;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.datastore.spi.TupleContext;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.type.GridType;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.type.Type;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class HashMapDialect implements GridDialect {

	private final MapDatastoreProvider provider;

	public HashMapDialect(MapDatastoreProvider provider) {
		this.provider = provider;
	}

	@Override
	public LockingStrategy getLockingStrategy(Lockable lockable, LockMode lockMode) {
		if ( lockMode == LockMode.PESSIMISTIC_FORCE_INCREMENT ) {
			return new PessimisticForceIncrementLockingStrategy( lockable, lockMode );
		}
		else if ( lockMode == LockMode.PESSIMISTIC_WRITE ) {
			return new MapPessimisticWriteLockingStrategy( lockable, lockMode );
		}
		else if ( lockMode == LockMode.PESSIMISTIC_READ ) {
			return new MapPessimisticReadLockingStrategy( lockable, lockMode );
		}
		else if ( lockMode == LockMode.OPTIMISTIC ) {
			return new OptimisticLockingStrategy( lockable, lockMode );
		}
		else if ( lockMode == LockMode.OPTIMISTIC_FORCE_INCREMENT ) {
			return new OptimisticForceIncrementLockingStrategy( lockable, lockMode );
		}
		return new MapPessimisticWriteLockingStrategy( lockable, lockMode );
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
		Map<String, Object> entityMap = provider.getEntityTuple( key );
		if ( entityMap == null ) {
			return null;
		}
		else {
			return new Tuple( new MapTupleSnapshot( entityMap ) );
		}
	}

	@Override
	public Tuple createTuple(EntityKey key) {
		HashMap<String, Object> tuple = new HashMap<String, Object>();
		provider.putEntity( key, tuple );
		return new Tuple( new MapTupleSnapshot( tuple ) );
	}

	@Override
	public void updateTuple(Tuple tuple, EntityKey key) {
		Map<String, Object> entityRecord = ( (MapTupleSnapshot) tuple.getSnapshot() ).getMap();
		MapHelpers.applyTupleOpsOnMap( tuple, entityRecord );
	}

	@Override
	public void removeTuple(EntityKey key) {
		provider.removeEntityTuple( key );
	}

	@Override
	public Association getAssociation(AssociationKey key, AssociationContext associationContext) {
		Map<RowKey, Map<String, Object>> associationMap = provider.getAssociation( key );
		return associationMap == null ? null : new Association( new MapAssociationSnapshot( associationMap ) );
	}

	@Override
	public Association createAssociation(AssociationKey key) {
		Map<RowKey, Map<String, Object>> associationMap = new HashMap<RowKey, Map<String, Object>>();
		provider.putAssociation( key, associationMap );
		return new Association( new MapAssociationSnapshot( associationMap ) );
	}

	@Override
	public void updateAssociation(Association association, AssociationKey key) {
		MapHelpers.updateAssociation( association, key );
	}

	@Override
	public void removeAssociation(AssociationKey key) {
		provider.removeAssociation( key );
	}

	@Override
	public Tuple createTupleAssociation(AssociationKey associationKey, RowKey rowKey) {
		return new Tuple( EmptyTupleSnapshot.SINGLETON );
	}

	@Override
	public void nextValue(RowKey key, IntegralDataTypeHolder value, int increment, int initialValue) {
		int nextValue = provider.getSharedAtomicInteger( key, initialValue, increment );
		value.initialize( nextValue );
	}

	@Override
	public GridType overrideType(Type type) {
		return null;
	}

	@Override
	public long countEntities(String indexedType) {
		Map<EntityKey, Map<String, Object>> entityMap = provider.getEntityMap();
		return entityMap.size();
	}

	@Override
	public ScrollableResults loadEntities(Class<?> indexedType, int idFetchSize) {
		Map<EntityKey, Map<String, Object>> entityMap = provider.getEntityMap();
		return new ScrollableResults() {

			@Override
			public boolean setRowNumber(int rowNumber) throws HibernateException {
				return false;
			}

			@Override
			public boolean scroll(int i) throws HibernateException {
				return false;
			}

			@Override
			public boolean previous() throws HibernateException {
				return false;
			}

			@Override
			public boolean next() throws HibernateException {
				return false;
			}

			@Override
			public boolean last() throws HibernateException {
				return false;
			}

			@Override
			public boolean isLast() throws HibernateException {
				return false;
			}

			@Override
			public boolean isFirst() throws HibernateException {
				return false;
			}

			@Override
			public Type getType(int i) {
				return null;
			}

			@Override
			public TimeZone getTimeZone(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getText(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getString(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Short getShort(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public int getRowNumber() throws HibernateException {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public Long getLong(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Locale getLocale(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Integer getInteger(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Float getFloat(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Double getDouble(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Date getDate(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Clob getClob(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Character getCharacter(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Calendar getCalendar(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Byte getByte(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Boolean getBoolean(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Blob getBlob(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public byte[] getBinary(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public BigInteger getBigInteger(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public BigDecimal getBigDecimal(int col) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Object get(int i) throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Object[] get() throws HibernateException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public boolean first() throws HibernateException {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public void close() throws HibernateException {
				// TODO Auto-generated method stub

			}

			@Override
			public void beforeFirst() throws HibernateException {
				// TODO Auto-generated method stub

			}

			@Override
			public void afterLast() throws HibernateException {
				// TODO Auto-generated method stub

			}
		};
	}

}
