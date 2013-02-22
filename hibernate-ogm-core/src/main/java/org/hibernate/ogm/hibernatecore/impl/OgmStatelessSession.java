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
package org.hibernate.ogm.hibernatecore.impl;

import java.io.Serializable;
import java.sql.Connection;

import org.hibernate.Criteria;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class OgmStatelessSession implements StatelessSession {

	private final StatelessSession delegate;
	private final OgmSessionFactory factory;

	public OgmStatelessSession(OgmSessionFactory factory, StatelessSession delegate) {
		this.factory = factory;
		this.delegate = delegate;
	}

	@Override
	public String getTenantIdentifier() {
		return delegate.getTenantIdentifier();
	}

	@Override
	public Transaction beginTransaction() {
		return delegate.beginTransaction();
	}

	@Override
	public Transaction getTransaction() {
		return delegate.getTransaction();
	}

	@Override
	public void close() {
		delegate.close();
	}

	@Override
	public Serializable insert(Object entity) {
		return delegate.insert( entity );
	}

	@Override
	public Query getNamedQuery(String queryName) {
		return delegate.getNamedQuery( queryName );
	}

	@Override
	public Serializable insert(String entityName, Object entity) {
		return delegate.insert( entityName, entity );
	}

	@Override
	public void update(Object entity) {
		delegate.update( entity );
	}

	@Override
	public Query createQuery(String queryString) {
		return delegate.createQuery( queryString );
	}

	@Override
	public void update(String entityName, Object entity) {
		delegate.update( entityName, entity );
	}

	@Override
	public SQLQuery createSQLQuery(String queryString) {
		return delegate.createSQLQuery( queryString );
	}

	@Override
	public void delete(Object entity) {
		delegate.delete( entity );
	}

	@Override
	public void delete(String entityName, Object entity) {
		delegate.delete( entityName, entity );
	}

	@Override
	public Criteria createCriteria(Class persistentClass) {
		return delegate.createCriteria( persistentClass );
	}

	@Override
	public Object get(String entityName, Serializable id) {
		return delegate.get( entityName, id );
	}

	@Override
	public Object get(Class entityClass, Serializable id) {
		return delegate.get( entityClass, id );
	}

	@Override
	public Criteria createCriteria(Class persistentClass, String alias) {
		return delegate.createCriteria( persistentClass, alias );
	}

	@Override
	public Object get(String entityName, Serializable id, LockMode lockMode) {
		return delegate.get( entityName, id, lockMode );
	}

	@Override
	public Object get(Class entityClass, Serializable id, LockMode lockMode) {
		return delegate.get( entityClass, id, lockMode );
	}

	@Override
	public Criteria createCriteria(String entityName) {
		return delegate.createCriteria( entityName );
	}

	@Override
	public void refresh(Object entity) {
		delegate.refresh( entity );
	}

	@Override
	public void refresh(String entityName, Object entity) {
		delegate.refresh( entityName, entity );
	}

	@Override
	public Criteria createCriteria(String entityName, String alias) {
		return delegate.createCriteria( entityName, alias );
	}

	@Override
	public void refresh(Object entity, LockMode lockMode) {
		delegate.refresh( entity, lockMode );
	}

	@Override
	public void refresh(String entityName, Object entity, LockMode lockMode) {
		delegate.refresh( entityName, entity, lockMode );
	}

	@Override
	public Connection connection() {
		return delegate.connection();
	}

}
