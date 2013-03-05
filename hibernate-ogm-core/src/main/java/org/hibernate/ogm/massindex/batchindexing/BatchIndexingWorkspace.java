/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2010, Red Hat, Inc. and/or its affiliates or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors.  All third-party contributions are
 * distributed under license by Red Hat, Inc.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.hibernate.ogm.massindex.batchindexing;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import org.hibernate.CacheMode;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.persister.OgmEntityPersister;
import org.hibernate.ogm.type.TypeTranslator;
import org.hibernate.search.SearchException;
import org.hibernate.search.backend.impl.batch.BatchBackend;
import org.hibernate.search.batchindexing.MassIndexerProgressMonitor;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.hibernate.search.exception.ErrorHandler;
import org.hibernate.search.util.logging.impl.Log;
import org.hibernate.search.util.logging.impl.LoggerFactory;

/**
 * This runnable will prepare a pipeline for batch indexing
 * of entities, managing the lifecycle of several ThreadPools.
 *
 * @author Sanne Grinovero
 */
public class BatchIndexingWorkspace implements Runnable {

	private static final Log log = LoggerFactory.make();

	private final SearchFactoryImplementor searchFactory;
	private final SessionFactory sessionFactory;

	//following order shows the 4 stages of an entity flowing to the index:
	private final ProducerConsumerQueue<List<?>> fromEntityToAddworkQueue;

	private final int objectLoadingThreadNum;
	private final int luceneWorkerBuildingThreadNum;
	private final Class<?> indexedType;

	// status control
	private final CountDownLatch producerEndSignal; //released when we stop adding Documents to Index 
	private final CountDownLatch endAllSignal; //released when we release all locks and IndexWriter

	// progress monitor
	private final MassIndexerProgressMonitor monitor;

	// loading options
	private final CacheMode cacheMode;
	private final int objectLoadingBatchSize;

	private final BatchBackend batchBackend;

	private final long objectsLimit;

	private final int idFetchSize;

	private final GridDialect gridDialect;

	private final TypeTranslator translator;

	public BatchIndexingWorkspace(GridDialect gridDialect, TypeTranslator translator, SearchFactoryImplementor searchFactoryImplementor,
								  SessionFactory sessionFactory,
								  Class<?> entityType,
								  int objectLoadingThreads,
								  int collectionLoadingThreads,
								  CacheMode cacheMode,
								  int objectLoadingBatchSize,
								  CountDownLatch endAllSignal,
								  MassIndexerProgressMonitor monitor,
								  BatchBackend backend,
								  long objectsLimit,
								  int idFetchSize) {

		this.gridDialect = gridDialect;
		this.translator = translator;
		this.indexedType = entityType;
		this.idFetchSize = idFetchSize;
		this.searchFactory = searchFactoryImplementor;
		this.sessionFactory = sessionFactory;

		//thread pool sizing:
		this.objectLoadingThreadNum = objectLoadingThreads;
		this.luceneWorkerBuildingThreadNum = collectionLoadingThreads;//collections are loaded as needed by building the document

		//loading options:
		this.cacheMode = cacheMode;
		this.objectLoadingBatchSize = objectLoadingBatchSize;
		this.batchBackend = backend;

		//pipelining queues:
		this.fromEntityToAddworkQueue = new ProducerConsumerQueue<List<?>>( objectLoadingThreadNum );

		//end signal shared with other instances:
		this.endAllSignal = endAllSignal;
		this.producerEndSignal = new CountDownLatch( luceneWorkerBuildingThreadNum );

		this.monitor = monitor;
		this.objectsLimit = objectsLimit;
	}

	private String table(SessionFactory sessionFactory, Class<?> indexedType) {
		OgmEntityPersister persister = (OgmEntityPersister) ((SessionFactoryImplementor) sessionFactory).getEntityPersister( indexedType.getName() );
		return persister.getTableName();
	}

	public void run() {
		ErrorHandler errorHandler = searchFactory.getErrorHandler();
		try {
			final String table = table( sessionFactory, indexedType );
			final SessionAwareRunnable consumer = new EntityConsumer( translator, indexedType, monitor, sessionFactory, producerEndSignal, searchFactory, cacheMode, batchBackend, errorHandler );
			gridDialect.forEachEntityKey( new OptionallyWrapInJTATransaction( sessionFactory, errorHandler, consumer ), table );
//			extracted();
		}
		catch ( RuntimeException re ) {
			//being this an async thread we want to make sure everything is somehow reported
			errorHandler.handleException( log.massIndexerUnexpectedErrorMessage() , re );
		}
		finally {
			endAllSignal.countDown();
		}
	}

	private void extracted() {
		try {
			producerEndSignal.await(); //await for all work being sent to the backend
			log.debugf( "All work for type %s has been produced", indexedType.getName() );
		}
		catch ( InterruptedException e ) {
			//restore interruption signal:
			Thread.currentThread().interrupt();
			throw new SearchException( "Interrupted on batch Indexing; index will be left in unknown state!", e );
		}
	}
}
