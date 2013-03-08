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

import java.util.List;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.ogm.datastore.map.impl.MapDatastoreProvider;
import org.hibernate.ogm.massindex.OgmMassIndexerFactory;
import org.hibernate.ogm.test.hsearch.Insurance;
import org.hibernate.ogm.test.simpleentity.OgmTestCase;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;
import org.hibernate.search.hcore.impl.MassIndexerFactoryIntegrator;
import org.junit.Test;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class MassIndexTest extends OgmTestCase {

	@Test
	public void testname() throws Exception {
		{
			Session session = openSession();
			Transaction transaction = session.beginTransaction();
			Insurance insurance = new Insurance();
			insurance.setName( "Insurance Corporation" );
			session.persist( insurance );
			transaction.commit();
			session.clear();
			session.close();
		}
		{
			FullTextSession session = Search.getFullTextSession( openSession() );
			session.purgeAll( Insurance.class );
			session.flushToIndexes();
			@SuppressWarnings("unchecked")
			List<Insurance> list = session.createQuery( "FROM Insurance" ).list();
			assertThat( list ).hasSize( 0 );
		}
		{
			FullTextSession session = Search.getFullTextSession( openSession() );
			session
				.createIndexer( Insurance.class )
				.purgeAllOnStart( true )
				.startAndWait();
		}
		{
			Session session = openSession();
			Transaction transaction = session.beginTransaction();
			@SuppressWarnings("unchecked")
			List<Insurance> list = session.createQuery( "FROM Insurance" ).list();
			assertThat( list ).hasSize( 1 );
			assertThat( list.get( 0 ).getName() ).isEqualTo( "Insurance Corporation" );
			transaction.commit();
			session.clear();
			session.close();
		}
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] { Insurance.class };
	}
}
