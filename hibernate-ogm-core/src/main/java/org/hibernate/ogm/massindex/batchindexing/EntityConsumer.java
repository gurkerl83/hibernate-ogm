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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockOptions;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.jdbc.TupleAsMapResultSet;
import org.hibernate.ogm.loader.OgmLoader;
import org.hibernate.ogm.loader.OgmLoadingContext;
import org.hibernate.ogm.persister.EntityKeyBuilder;
import org.hibernate.ogm.persister.OgmEntityPersister;
import org.hibernate.ogm.type.TypeTranslator;
import org.hibernate.ogm.type.descriptor.GridTypeDescriptor;
import org.hibernate.ogm.type.impl.TypeTranslatorImpl;
import org.hibernate.search.backend.AddLuceneWork;
import org.hibernate.search.backend.impl.batch.BatchBackend;
import org.hibernate.search.batchindexing.MassIndexerProgressMonitor;
import org.hibernate.search.bridge.TwoWayFieldBridge;
import org.hibernate.search.bridge.spi.ConversionContext;
import org.hibernate.search.bridge.util.impl.ContextualExceptionBridgeHelper;
import org.hibernate.search.engine.impl.HibernateSessionLoadingInitializer;
import org.hibernate.search.engine.spi.DocumentBuilderIndexedEntity;
import org.hibernate.search.engine.spi.EntityIndexBinder;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.hibernate.search.exception.ErrorHandler;
import org.hibernate.search.indexes.interceptor.EntityIndexingInterceptor;
import org.hibernate.search.indexes.interceptor.IndexingOverride;
import org.hibernate.search.spi.InstanceInitializer;
import org.hibernate.search.util.impl.HibernateHelper;
import org.hibernate.search.util.logging.impl.Log;
import org.hibernate.search.util.logging.impl.LoggerFactory;
import org.hibernate.tuple.IdentifierProperty;
import org.hibernate.type.Type;

/**
 * Component of batch-indexing pipeline, using chained producer-consumers.
 * This Runnable will consume entities taken one-by-one from the queue
 * and produce for each entity an AddLuceneWork to the output queue.
 *
 * @author Sanne Grinovero
 */
public class EntityConsumer implements SessionAwareRunnable {

	private static final Log log = LoggerFactory.make();

	private final SessionFactory sessionFactory;
	private final Map<Class<?>, EntityIndexBinder> entityIndexBinders;
	private final MassIndexerProgressMonitor monitor;
	private final CacheMode cacheMode;
	private final CountDownLatch producerEndSignal;
	private final BatchBackend backend;
	private final ErrorHandler errorHandler;

	private final Class<?> indexedType;

	private final TypeTranslator translator;

	public EntityConsumer(
			TypeTranslator translator, Class<?> indexedType, MassIndexerProgressMonitor monitor,
			SessionFactory sessionFactory,
			CountDownLatch producerEndSignal,
			SearchFactoryImplementor searchFactory,
			CacheMode cacheMode,
			BatchBackend backend,
			ErrorHandler errorHandler) {
		this.translator = translator;
		this.indexedType = indexedType;
		this.monitor = monitor;
		this.sessionFactory = sessionFactory;
		this.producerEndSignal = producerEndSignal;
		this.cacheMode = cacheMode;
		this.backend = backend;
		this.errorHandler = errorHandler;
		this.entityIndexBinders = searchFactory.getIndexBindingForEntity();
	}

	private void index(Session session, Object entity) {
		try {
			final InstanceInitializer sessionInitializer = new HibernateSessionLoadingInitializer( (SessionImplementor) session );
			final ConversionContext contextualBridge = new ContextualExceptionBridgeHelper();

			// trick to attach the objects to session:
			session.buildLockRequest( LockOptions.NONE ).lock( entity );
			index( entity, session, sessionInitializer, contextualBridge );
			monitor.documentsBuilt( 1 );
			session.clear();
		}
		catch ( InterruptedException e ) {
			Thread.currentThread().interrupt();
		}
	}

	private void index(Object entity, Session session, InstanceInitializer sessionInitializer, ConversionContext conversionContext)
			throws InterruptedException {
		Class<?> clazz = HibernateHelper.getClass( entity );
		EntityIndexBinder entityIndexBinding = entityIndexBinders.get( clazz );
		// it might be possible to receive not-indexes subclasses of the currently indexed type;
		// being not-indexed, we skip them.
		// FIXME for improved performance: avoid loading them in an early phase.
		if ( entityIndexBinding != null ) {
			EntityIndexingInterceptor interceptor = entityIndexBinding.getEntityIndexingInterceptor();
			if (isNotSkippable(interceptor, entity)) {
				Serializable id = session.getIdentifier( entity );
				AddLuceneWork addWork = createAddLuceneWork( entity, sessionInitializer, conversionContext, id, clazz, entityIndexBinding );
				backend.enqueueAsyncWork( addWork );
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private AddLuceneWork createAddLuceneWork(Object entity, InstanceInitializer sessionInitializer,
			ConversionContext conversionContext, Serializable id, Class<?> clazz, EntityIndexBinder entityIndexBinding) {
		DocumentBuilderIndexedEntity docBuilder = entityIndexBinding.getDocumentBuilder();
		String idInString = idInString( conversionContext, id, clazz, docBuilder );
		//depending on the complexity of the object graph going to be indexed it's possible
		//that we hit the database several times during work construction.
		return docBuilder.createAddWork( clazz, entity, id, idInString, sessionInitializer, conversionContext );
	}

	private String idInString(ConversionContext conversionContext, Serializable id, Class<?> clazz, DocumentBuilderIndexedEntity docBuilder) {
		conversionContext.pushProperty( docBuilder.getIdKeywordName() );
		try {
			String idInString = conversionContext
					.setClass( clazz )
					.twoWayConversionContext( docBuilder.getIdBridge() )
					.objectToString( id );
			return idInString;
		}
		finally {
			conversionContext.popProperty();
		}
	}

	private boolean isNotSkippable(EntityIndexingInterceptor interceptor, Object entity) {
		if ( interceptor == null ) {
			return true;
		}
		else {
			return !isSkippable( interceptor.onAdd( entity ) );
		}
	}

	private boolean isSkippable(IndexingOverride indexingOverride) {
		switch ( indexingOverride ) {
		case REMOVE:
		case SKIP:
			return true;
		default:
			return false;
		}
	}

	private Transaction beginTransaction(Session session) throws ClassNotFoundException, NoSuchMethodException,
			IllegalAccessException, InvocationTargetException {
		Transaction transaction = Helper.getTransactionAndMarkForJoin( session );
		transaction.begin();
		return transaction;
	}

	private Session openSession(Session upperSession) {
		Session session = upperSession;
		if ( upperSession == null ) {
			session = sessionFactory.openSession();
		}
		session.setFlushMode( FlushMode.MANUAL );
		session.setCacheMode( cacheMode );
		session.setDefaultReadOnly( true );
		return session;
	}

	private void close(Session upperSession, Session session) {
		if ( upperSession == null ) {
			session.close();
		}
	}

	@Override
	public void consume(Session upperSession, Tuple tuple) {
		Session session = openSession( upperSession );
		try {
			Transaction transaction = beginTransaction( session );
			index( session, entity( session, tuple ) );
			transaction.commit();
		}
		catch ( Throwable e ) {
			errorHandler.handleException( log.massIndexerUnexpectedErrorMessage(), e );
		}
		finally {
			producerEndSignal.countDown();
			close( upperSession, session );
			log.debug( "finished" );
		}
	}

	private Object entity(Session session, Tuple tuple) {
		OgmEntityPersister persister = (OgmEntityPersister) ((SessionFactoryImplementor) sessionFactory).getEntityPersister( indexedType.getName() );
		OgmLoader loader = new OgmLoader( new OgmEntityPersister[] { persister } );
		OgmLoadingContext ogmLoadingContext = new OgmLoadingContext();
		List<Tuple> tuples = new ArrayList<Tuple>();
		tuples.add( tuple );
		ogmLoadingContext.setTuples(tuples);
		List<Object> entities = loader.loadEntities( (SessionImplementor) session, LockOptions.NONE, ogmLoadingContext );
		return entities.get( 0 );
	}

}
