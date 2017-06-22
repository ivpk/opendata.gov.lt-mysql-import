# coding: utf-8

from __future__ import unicode_literals

import os
import hashlib
import collections
import gettext
import contextlib

import psycopg2
import psycopg2.extensions
import pytest
import webtest
import pkg_resources as pres
import sqlalchemy as sa
import pylons
import ckan.config.middleware

from odgovltimport import CkanAPI, CkanSync


class CKANTestApp(webtest.TestApp):
    '''A wrapper around webtest.TestApp

    It adds some convenience methods for CKAN
    '''

    _flask_app = None

    @property
    def flask_app(self):
        if not self._flask_app:
            self._flask_app = self.app.apps['flask_app']._wsgi_app
        return self._flask_app


@pytest.fixture
def postgres():
    dbname = 'odgovlt_mysql_import_tests'
    with contextlib.closing(psycopg2.connect(dbname='postgres')) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with contextlib.closing(conn.cursor()) as curs:
            curs.execute('DROP DATABASE IF EXISTS ' + dbname)
            curs.execute('CREATE DATABASE ' + dbname + ' ENCODING = utf8')
    return 'postgresql:///%s' % dbname


@pytest.fixture
def app(postgres):
    global_config = {
        '__file__': '',
        'here': os.path.dirname(__file__),
        'ckan.site_url': 'http://localhost',
        'sqlalchemy.url': postgres,
    }
    app_config = {
        'who.config_file': pres.resource_filename('ckan.config', 'who.ini'),
        'beaker.session.secret': 'secret',
    }
    app = ckan.config.middleware.make_app(global_config, **app_config)
    app = CKANTestApp(app)

    import ckan.model as model

    model.repo.init_db()

    pylons.translator = gettext.NullTranslations()

    return app


@pytest.fixture
def db():
    engine = sa.create_engine('sqlite://')
    db = sa.MetaData()

    with open(os.path.join(os.path.dirname(__file__), 'schema.sql')) as f:
        engine.raw_connection().executescript(f.read())

    db.reflect(bind=engine)

    return collections.namedtuple('DB', ('engine', 'metadata'))(engine, db)


def test_me(app, db):
    conn = db.engine.connect()

    user_id, = conn.execute(db.metadata.tables['t_user'].insert(), {
        'LOGIN': 'vardenis',
        'FIRST_NAME': 'Vardenis',
        'LAST_NAME': 'Pavardenis',
        'EMAIL': 'vardenis@example.com',
        'TELEFONAS': '+37067000000',
        'PASS': hashlib.md5(b'secret').hexdigest(),
    }).inserted_primary_key

    organization_id, = conn.execute(db.metadata.tables['t_istaiga'].insert(), {
        'KODAS': '1048',
        'PAVADINIMAS': 'VĮ Registrų centras',
        'ADRESAS': 'Adreso g., Vilnius',
    }).inserted_primary_key

    conn.execute(db.metadata.tables['t_rinkmena'].insert(), [
        {
            'USER_ID': user_id,
            'istaiga_id': organization_id,
            'PAVADINIMAS': 'Adresų registras',
            'SANTRAUKA': 'Gatvių ir adresų duomenys.',
            'R_ZODZIAI': 'adresas, administraciniai vienetai',
        },
    ])

    ckanapi = CkanAPI()

    sync = CkanSync(ckanapi, db.metadata, conn)

    # Do initial syncronisation.
    sync.sync_datasets()

    # Try to update changes on existing dataase.
    sync.sync_datasets()

    # Packages.
    packages = [ckanapi.package_show(id=x) for x in ckanapi.package_list()]
    assert [(x['title'],) for x in packages] == [
        ('Adresų registras',),
    ]

    # Tags.
    tags = ckanapi.package_show(id='adresu-registras')['tags']
    assert sorted([x['name'] for x in tags]) == ['administraciniai vienetai', 'adresas']

    # Users.
    assert [(x['name'], x['fullname']) for x in ckanapi.user_list()] == [
        ('importbot', None),
        ('vardenis', 'Vardenis Pavardenis'),
    ]

    # Organizations.
    organizations = [ckanapi.organization_show(id=x) for x in ckanapi.organization_list()]
    assert [(x['title'],) for x in organizations] == [
        ('VĮ Registrų centras',)
    ]
