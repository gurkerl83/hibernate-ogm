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
package org.hibernate.ogm.massindex;

import java.util.concurrent.Future;

import org.hibernate.CacheMode;
import org.hibernate.SessionFactory;
import org.hibernate.search.MassIndexer;
import org.hibernate.search.batchindexing.MassIndexerProgressMonitor;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class OgmMassIndexer implements MassIndexer {

	private final SearchFactoryImplementor searchFactory;
	private final SessionFactory sessionFactory;
	private final Class<?>[] entities;

	public OgmMassIndexer(SearchFactoryImplementor searchFactory, SessionFactory sessionFactory, Class<?>... entities) {
		this.searchFactory = searchFactory;
		this.sessionFactory = sessionFactory;
		this.entities = entities;
	}

	@Override
	public MassIndexer threadsToLoadObjects(int numberOfThreads) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MassIndexer batchSizeToLoadObjects(int batchSize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MassIndexer threadsForSubsequentFetching(int numberOfThreads) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@Deprecated
	public MassIndexer threadsForIndexWriter(int numberOfThreads) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MassIndexer progressMonitor(MassIndexerProgressMonitor monitor) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MassIndexer cacheMode(CacheMode cacheMode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MassIndexer optimizeOnFinish(boolean optimize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MassIndexer optimizeAfterPurge(boolean optimize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MassIndexer purgeAllOnStart(boolean purgeAll) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MassIndexer limitIndexedObjectsTo(long maximum) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Future<?> start() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void startAndWait() throws InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public MassIndexer idFetchSize(int idFetchSize) {
		// TODO Auto-generated method stub
		return null;
	}

}
