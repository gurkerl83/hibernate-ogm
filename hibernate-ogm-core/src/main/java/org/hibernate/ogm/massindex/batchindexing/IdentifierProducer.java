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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.search.batchindexing.MassIndexerProgressMonitor;
import org.hibernate.search.exception.ErrorHandler;
import org.hibernate.search.util.logging.impl.Log;
import org.hibernate.search.util.logging.impl.LoggerFactory;

/**
 * This Runnable is going to feed the indexing queue
 * with the identifiers of all the entities going to be indexed.
 * This step in the indexing process is not parallel (should be
 * done by one thread per type) so that a single transaction is used
 * to define the group of entities to be indexed.
 * Produced identifiers are put in the destination queue grouped in List
 * instances: the reason for this is to load them in batches
 * in the next step and reduce contention on the queue.
 * 
 * @author Sanne Grinovero
 */
public class IdentifierProducer implements SessionAwareRunnable {
	
	private static final Log log = LoggerFactory.make();

	private final ProducerConsumerQueue<List<Serializable>> destination;
	private final SessionFactory sessionFactory;
	private final int batchSize;
	private final Class<?> indexedType;
	private final MassIndexerProgressMonitor monitor;
	private final long objectsLimit;
	private final ErrorHandler errorHandler;
	private final int idFetchSize;

	private GridDialect gridDialect;

	private String idNameOfIndexedType;

	/**
	 * @param gridDialect 
	 * @param idNameOfIndexedType 
	 * @param fromIdentifierListToEntities the target queue where the produced identifiers are sent to
	 * @param sessionFactory the Hibernate SessionFactory to use to load entities
	 * @param objectLoadingBatchSize affects mostly the next consumer: IdentifierConsumerEntityProducer
	 * @param indexedType the entity type to be loaded
	 * @param monitor to monitor indexing progress
	 * @param objectsLimit if not zero
	 * @param errorHandler how to handle unexpected errors
	 */
	public IdentifierProducer(
			GridDialect gridDialect, String idNameOfIndexedType, ProducerConsumerQueue<List<Serializable>> fromIdentifierListToEntities,
			SessionFactory sessionFactory,
			int objectLoadingBatchSize,
			Class<?> indexedType, MassIndexerProgressMonitor monitor,
			long objectsLimit, ErrorHandler errorHandler, int idFetchSize) {
				this.gridDialect = gridDialect;
				this.idNameOfIndexedType = idNameOfIndexedType;
				this.destination = fromIdentifierListToEntities;
				this.sessionFactory = sessionFactory;
				this.batchSize = objectLoadingBatchSize;
				this.indexedType = indexedType;
				this.monitor = monitor;
				this.objectsLimit = objectsLimit;
				this.errorHandler = errorHandler;
				this.idFetchSize = idFetchSize;
				log.trace( "created" );
	}
	
	public void run(Session upperSession) {
		log.trace( "started" );
		try {
			inTransactionWrapper(upperSession);
		}
		catch (Throwable e) {
			errorHandler.handleException( log.massIndexerUnexpectedErrorMessage() , e );
		}
		finally{
			destination.producerStopping();
		}
		log.trace( "finished" );
	}

	private void inTransactionWrapper(Session upperSession) throws Exception {
		Session session = upperSession;
		if (upperSession == null) {
			session = sessionFactory.openSession();
		}
		try {
			Transaction transaction = Helper.getTransactionAndMarkForJoin( session );
			transaction.begin();
			loadAllIdentifiers( session );
			transaction.commit();
		} catch (InterruptedException e) {
			// just quit
			Thread.currentThread().interrupt();
		}
		finally {
			if (upperSession == null) {
				session.close();
			}
		}
	}

	private void loadAllIdentifiers(final Session session) throws InterruptedException {
		long totalCount = gridDialect.countEntities( idNameOfIndexedType );
		if ( objectsLimit != 0 && objectsLimit < totalCount ) {
			totalCount = objectsLimit;
		}
		if ( log.isDebugEnabled() )
			log.debugf( "going to fetch %d primary keys", totalCount);
		monitor.addToTotalCount( totalCount );

		ScrollableResults results = gridDialect.loadEntities( indexedType, idFetchSize );
		List<Serializable> destinationList = new ArrayList<Serializable>( batchSize );
		long counter = 0;
		try {
			while ( results.next() ) {
				Serializable id = (Serializable) results.get( 0 );
				destinationList.add( id );
				if ( destinationList.size() == batchSize ) {
					enqueueList( destinationList );
					destinationList = new ArrayList<Serializable>( batchSize ); 
				}
				counter++;
				if ( counter == totalCount ) {
					break;
				}
			}
		}
		finally {
			results.close();
		}
		enqueueList( destinationList );
	}
	
	private void enqueueList(final List<Serializable> idsList) throws InterruptedException {
		if ( ! idsList.isEmpty() ) {
			destination.put( idsList );
			log.tracef( "produced a list of ids %s", idsList );
		}
	}

}
