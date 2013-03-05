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

import java.util.Map;

import org.hibernate.ogm.datastore.impl.DatastoreServices;
import org.hibernate.ogm.type.TypeTranslator;
import org.hibernate.search.hcore.impl.MassIndexerFactoryIntegrator;
import org.hibernate.search.spi.MassIndexerFactory;
import org.hibernate.service.spi.BasicServiceInitiator;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class OgmMassIndexerInitiator implements BasicServiceInitiator<MassIndexerFactory> {

	public static final OgmMassIndexerInitiator INSTANCE = new OgmMassIndexerInitiator();

	@Override
	public Class<MassIndexerFactory> getServiceInitiated() {
		return MassIndexerFactory.class;
	}

	@Override
	public MassIndexerFactory initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		String factoryClassName = (String) configurationValues.get( MassIndexerFactoryIntegrator.MASS_INDEXER_FACTORY_CLASSNAME );
		if (factoryClassName == null) {
			DatastoreServices services = registry.getService( DatastoreServices.class );
			TypeTranslator translator = registry.getService( TypeTranslator.class );
			return new OgmMassIndexerFactory( services.getGridDialect(), translator );
		} else {
			return new MassIndexerFactoryIntegrator().initiateService( configurationValues, registry );
		}
	}


}
