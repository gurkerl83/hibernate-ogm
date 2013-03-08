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
package org.hibernate.ogm.test.massindex;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import javax.persistence.EntityManager;

import org.hibernate.Session;
import org.hibernate.ogm.test.hsearch.Insurance;
import org.hibernate.ogm.test.id.NewsID;
import org.hibernate.ogm.test.massindex.model.IndexedLabel;
import org.hibernate.ogm.test.massindex.model.IndexedNews;
import org.hibernate.ogm.test.utils.jpa.JpaTestCase;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;
import org.junit.Test;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class SnippetTest extends JpaTestCase {

	@Test
	public void testEntityWithAssociationMassIndexing() throws Exception {
		{
			List<IndexedLabel> labes = Arrays.asList( new IndexedLabel( "massindex" ), new IndexedLabel( "test" ) );
			IndexedNews news = new IndexedNews( new NewsID( "title", "author" ), "content" );
			boolean operationSuccessful = false;
			try {
				getTransactionManager().begin();
				EntityManager em = createEntityManager();
				news.setLabels( labes );
				em.persist( news );
				operationSuccessful = true;
			}
			finally {
				commitOrRollback( operationSuccessful );
			}
		}
		{
			purgeAll( IndexedNews.class );
			purgeAll( IndexedLabel.class );
			startAndWaitMassIndexing( IndexedNews.class );
		}
		{
			boolean operationSuccessful = false;
			try {
				getTransactionManager().begin();
				Session session = createSession();
				@SuppressWarnings("unchecked")
				List<IndexedNews> list = session.createQuery( "FROM " + IndexedNews.class.getSimpleName() ).list();
				assertThat( list ).hasSize( 1 );
				List<IndexedLabel> labels = list.get( 0 ).getLabels();
				assertThat( labels ).hasSize( 2 );
				assertThat( labels ).contains( new IndexedLabel( "massindex" ), new IndexedLabel( "test" ) );
				operationSuccessful = true;
			}
			finally {
				commitOrRollback( operationSuccessful );
			}
		}
	}

	private EntityManager createEntityManager() {
		return getFactory().createEntityManager();
	}

	private void startAndWaitMassIndexing(Class<?> entityType) throws InterruptedException {
		FullTextSession session = Search.getFullTextSession( createSession() );
		session.createIndexer( entityType ).purgeAllOnStart( true ).startAndWait();
	}

	private Session createSession() {
		return (Session) createEntityManager().getDelegate();
	}

	private void purgeAll(Class<?> entityType) {
		FullTextSession session = Search.getFullTextSession( createSession() );
		session.purgeAll( entityType );
		session.flushToIndexes();
		@SuppressWarnings("unchecked")
		List<Insurance> list = session.createQuery( "FROM " + entityType.getSimpleName() ).list();
		assertThat( list ).hasSize( 0 );
	}

	@Override
	public Class<?>[] getEntities() {
		return new Class<?>[] { Insurance.class, IndexedNews.class, IndexedLabel.class };
	}
}
