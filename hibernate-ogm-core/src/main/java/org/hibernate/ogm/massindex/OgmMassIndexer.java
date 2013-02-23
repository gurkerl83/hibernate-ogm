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
import org.hibernate.ogm.dialect.GridDialect;
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
	private final GridDialect gridDialect;

	private int threadsToLoad = 2;
	private int batchSize = 10;
	private int threadsForSubsequent;
	private int threadsForIndex;
	private MassIndexerProgressMonitor monitor;
	private CacheMode cacheMode;
	private boolean optimizeOnFinish;
	private boolean optimizeAfterPurge;
	private boolean purgeAllOnStart;
	private long limitIndexedObjectsTo;
	private int idFetchSize;

	public OgmMassIndexer(GridDialect gridDialect, SearchFactoryImplementor searchFactory, SessionFactory sessionFactory, Class<?>... entities) {
		this.gridDialect = gridDialect;
		this.searchFactory = searchFactory;
		this.sessionFactory = sessionFactory;
		this.entities = entities;
	}

	@Override
	public MassIndexer threadsToLoadObjects(int threadsToLoad) {
		if ( threadsToLoad < 1 ) {
			throw new IllegalArgumentException( "numberOfThreads must be at least 1" );
		}
		this.threadsToLoad = threadsToLoad;
		return this;
	}

	@Override
	public MassIndexer batchSizeToLoadObjects(int batchSize) {
		if ( batchSize < 1 ) {
			throw new IllegalArgumentException( "batchSize must be at least 1" );
		}
		this.batchSize = batchSize;
		return this;
	}

	@Override
	public MassIndexer threadsForSubsequentFetching(int threadsForSubsequent) {
		if ( threadsForSubsequent < 1 ) {
			throw new IllegalArgumentException( "numberOfThreads must be at least 1" );
		}
		this.threadsForSubsequent = threadsForSubsequent;
		return this;
	}

	@Override
	@Deprecated
	public MassIndexer threadsForIndexWriter(int threadsForIndex) {
		this.threadsForIndex = threadsForIndex;
		return this;
	}

	@Override
	public MassIndexer progressMonitor(MassIndexerProgressMonitor monitor) {
		this.monitor = monitor;
		return this;
	}

	@Override
	public MassIndexer cacheMode(CacheMode cacheMode) {
		if ( cacheMode == null ) {
			throw new IllegalArgumentException( "cacheMode must not be null" );
		}
		this.cacheMode = cacheMode;
		return this;
	}

	@Override
	public MassIndexer optimizeOnFinish(boolean optimizeOnFinish) {
		this.optimizeOnFinish = optimizeOnFinish;
		return this;
	}

	@Override
	public MassIndexer optimizeAfterPurge(boolean optimizeAfterPurge) {
		this.optimizeAfterPurge = optimizeAfterPurge;
		return this;
	}

	@Override
	public MassIndexer purgeAllOnStart(boolean purgeAllOnStart) {
		this.purgeAllOnStart = purgeAllOnStart;
		return this;
	}

	@Override
	public MassIndexer limitIndexedObjectsTo(long limitIndexedObjectsTo) {
		this.limitIndexedObjectsTo = limitIndexedObjectsTo;
		return this;
	}

	@Override
	public MassIndexer idFetchSize(int idFetchSize) {
		this.idFetchSize = idFetchSize;
		return this;
	}

	@Override
	public Future<?> start() {
		return null;
	}

	@Override
	public void startAndWait() throws InterruptedException {
	}

}
