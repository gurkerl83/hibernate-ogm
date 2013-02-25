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
import org.hibernate.search.impl.MassIndexerImpl;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class OgmMassIndexer implements MassIndexer {

	private final SearchFactoryImplementor searchFactory;
	private final SessionFactory sessionFactory;
	private final Class<?>[] entities;
	private final GridDialect gridDialect;

	private final MassIndexer indexer;

	public OgmMassIndexer(GridDialect gridDialect, SearchFactoryImplementor searchFactory, SessionFactory sessionFactory, Class<?>... entities) {
		this.gridDialect = gridDialect;
		this.searchFactory = searchFactory;
		this.sessionFactory = sessionFactory;
		this.entities = entities;
		this.indexer = new MassIndexerImpl( searchFactory, sessionFactory, entities );
	}

	public MassIndexer threadsToLoadObjects(int numberOfThreads) {
		return indexer.threadsToLoadObjects( numberOfThreads );
	}

	public MassIndexer batchSizeToLoadObjects(int batchSize) {
		return indexer.batchSizeToLoadObjects( batchSize );
	}

	public MassIndexer threadsForSubsequentFetching(int numberOfThreads) {
		return indexer.threadsForSubsequentFetching( numberOfThreads );
	}

	public MassIndexer threadsForIndexWriter(int numberOfThreads) {
		return indexer.threadsForIndexWriter( numberOfThreads );
	}

	public MassIndexer progressMonitor(MassIndexerProgressMonitor monitor) {
		return indexer.progressMonitor( monitor );
	}

	public MassIndexer cacheMode(CacheMode cacheMode) {
		return indexer.cacheMode( cacheMode );
	}

	public MassIndexer optimizeOnFinish(boolean optimize) {
		return indexer.optimizeOnFinish( optimize );
	}

	public MassIndexer optimizeAfterPurge(boolean optimize) {
		return indexer.optimizeAfterPurge( optimize );
	}

	public MassIndexer purgeAllOnStart(boolean purgeAll) {
		return indexer.purgeAllOnStart( purgeAll );
	}

	public MassIndexer limitIndexedObjectsTo(long maximum) {
		return indexer.limitIndexedObjectsTo( maximum );
	}

	public Future<?> start() {
		return indexer.start();
	}

	public void startAndWait() throws InterruptedException {
		indexer.startAndWait();
	}

	public MassIndexer idFetchSize(int idFetchSize) {
		return indexer.idFetchSize( idFetchSize );
	}

}
