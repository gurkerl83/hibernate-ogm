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

import java.sql.Connection;

import org.hibernate.StatelessSession;
import org.hibernate.StatelessSessionBuilder;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class OgmStatelessSessionBuilderDelegator implements StatelessSessionBuilder {

	private final StatelessSessionBuilder builder;
	private final OgmSessionFactory factory;

	public OgmStatelessSessionBuilderDelegator(StatelessSessionBuilder builder, OgmSessionFactory factory) {
		this.builder = builder;
		this.factory = factory;
	}

	@Override
	public StatelessSession openStatelessSession() {
		return new OgmStatelessSession( factory, builder.openStatelessSession() );
	}

	@Override
	public StatelessSessionBuilder connection(Connection connection) {
		builder.connection( connection );
		return this;
	}

	@Override
	public StatelessSessionBuilder tenantIdentifier(String tenantIdentifier) {
		builder.tenantIdentifier( tenantIdentifier );
		return this;
	}

}
