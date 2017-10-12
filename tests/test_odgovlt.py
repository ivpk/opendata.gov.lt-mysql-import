# -*- coding: utf-8 -*-

import collections
import contextlib
import gettext
import hashlib
import json
import os

from ckan import model
from ckan.tests.factories import Organization
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.tests.factories import (HarvestSourceObj, HarvestJobObj, HarvestObjectObj)
from ckanext.harvest.tests.harvesters import mock_ckan
from ckanext.harvest.tests.lib import run_harvest
import ckan.config.middleware
import ckanext.harvest.model as harvest_model
import mock
import pkg_resources as pres
import psycopg2
import psycopg2.extensions
import pylons
import pytest
import sqlalchemy as sa
import webtest

from odgovlt import ODGovLt


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


def was_last_job_considered_error_free():
    last_job = model.Session.query(harvest_model.HarvestJob) \
                    .order_by(harvest_model.HarvestJob.created.desc()) \
                    .first()
    job = mock.MagicMock()
    job.source = last_job.source
    job.id = ''
    return bool(CKANHarvester._last_error_free_job(job))


@pytest.fixture
def db():
    engine = sa.create_engine('sqlite://')
    db = sa.MetaData()

    with open(os.path.join(os.path.dirname(__file__), 'schema.sql')) as f:
        engine.raw_connection().executescript(f.read())

    db.reflect(bind=engine)

    return collections.namedtuple('DB', ('engine', 'metadata'))(engine, db)


@pytest.fixture
def postgres():
    dbname = 'odgovlt_mysql_import_tests'
    with contextlib.closing(psycopg2.connect('postgresql:///postgres')) as conn:
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
        'ckan.plugins': 'harvest odgovlt_harvester',
    }
    app_config = {
        'who.config_file': pres.resource_filename('ckan.config', 'who.ini'),
        'beaker.session.secret': 'secret',
    }
    app = ckan.config.middleware.make_app(global_config, **app_config)
    app = CKANTestApp(app)

    import ckan.model as model

    model.repo.init_db()

    # harvest_model.setup()

    pylons.translator = gettext.NullTranslations()

    return app


@pytest.fixture
def ckanfixture(db):
    conn = db.engine

    user_id, = conn.execute(db.metadata.tables['t_user'].insert(), {
        'LOGIN': 'vardenis',
        'FIRST_NAME': 'Vardenis',
        'LAST_NAME': 'Pavardenis',
        'EMAIL': 'vardenis@example.com',
        'TELEFONAS': '+37067000000',
        'PASS': hashlib.md5(b'secret').hexdigest(),
    }).inserted_primary_key

    conn.execute('''
        INSERT INTO t_rinkmena VALUES(1, 'Šilumos tiekimo licencijas\
turinčių įmonių sąrašas', '​Šilumos tiekimo\
licencijas turinčių įmonių sąrašas',\
'http://www.vkekk.lt/siluma/Puslapiai/licencijavimas/licenciju-turetojai.aspx',\
'jonaiste.jusionyte@regula.lt')
        ''')
    conn.execute('''
        INSERT INTO t_rinkmena VALUES(2, '2014 m. vidutinio\
metinio paros eismo intensyvumo duomenys', 'Pateikiama\
informacija: kelio numeris, ruožo pradžia, ruožo\
pabaiga, vidutinis metinis paros eismo \
intensyvumas, metai, automobilių tipai',\
'http://lakd.lrv.lt/lt/atviri-duomenys/vidutinio-m\
etinio-paros-eismo-intensyvumo-valstybines-reik\
smes-keliuose-duomenys-2013-m', 'vytautas.timukas@lakd.lt')
        ''')


def test_gather(app, db):
    class Tables(object):
        rinkmena = db.metadata.tables['t_rinkmena']

    clause = Tables.rinkmena.select()
    opendatagov_database = dict()
    for row in db.engine.execute(clause):
        opendatagov_database[row[0]] = row

    source = HarvestSourceObj(url=db.engine)
    job = HarvestJobObj(source=source)
    harvester = ODGovLt()
    obj_ids = harvester.gather_stage(job)
    assert job.gather_errors == []
    assert isinstance(obj_ids, list)
    assert len(obj_ids) == len(opendatagov_database)
    harvest_object = harvest_model.HarvestObject.get(obj_ids[0])
    assert harvest_object.guid == str(opendatagov_database.keys()[0])
    content = json.loads(harvest_object.content)
    assert content['ID'] == opendatagov_database[1][0]
    assert content['PAVADINIMAS'] == opendatagov_database[1][1]
    assert content['SANTRAUKA'] == opendatagov_database[1][2]
    harvest_object = harvest_model.HarvestObject.get(obj_ids[1])
    assert harvest_object.guid == str(opendatagov_database.keys()[1])
    content = json.loads(harvest_object.content)
    assert content['ID'] == opendatagov_database[2][0]
    assert content['PAVADINIMAS'] == opendatagov_database[2][1]
    assert content['SANTRAUKA'] == opendatagov_database[2][2]


def test_fetch():
    source = HarvestSourceObj(url='http://localhost:%s/' % mock_ckan.PORT)
    job = HarvestJobObj(source=source)
    harvest_object = HarvestObjectObj(
        guid=mock_ckan.DATASETS[0]['id'],
        job=job,
        content=json.dumps(mock_ckan.DATASETS[0]))
    harvester = CKANHarvester()
    result = harvester.fetch_stage(harvest_object)
    assert harvest_object.errors == []
    assert result


def test_import():
    db_url = 'sqlite:///opendatagov.db'
    con = sa.create_engine(db_url)
    meta = sa.MetaData(bind=con, reflect=True)

    class Tables(object):
        rinkmena = meta.tables['t_rinkmena']

    clause = Tables.rinkmena.select().where(Tables.rinkmena.c.ID == 1)
    opendatagov_database = dict()
    for row in con.execute(clause):
        for row_name in row.keys():
            opendatagov_database[row_name] = row[row_name]
    org = Organization()
    harvest_object = HarvestObjectObj(
        guid=opendatagov_database['ID'],
        content=json.dumps(opendatagov_database),
        job__source__owner_org=org['id'])
    harvester = CKANHarvester()
    result = harvester.import_stage(harvest_object)
    assert harvest_object.errors == []
    assert result
    assert harvest_object.package_id
    clause = Tables.rinkmena.select().where(Tables.rinkmena.c.ID == 2)
    opendatagov_database = dict()
    for row in con.execute(clause):
        for row_name in row.keys():
            opendatagov_database[row_name] = row[row_name]
    org = Organization()
    harvest_object = HarvestObjectObj(
        guid=opendatagov_database['ID'],
        content=json.dumps(opendatagov_database),
        job__source__owner_org=org['id'])
    harvester = CKANHarvester()
    result = harvester.import_stage(harvest_object)
    assert harvest_object.errors == []
    assert result
    assert harvest_object.package_id


def test_harvest():
    db_url = 'sqlite:///opendatagov.db'
    con = sa.create_engine(db_url)
    meta = sa.MetaData(bind=con, reflect=True)

    class Tables(object):
        rinkmena = meta.tables['t_rinkmena']

    clause = Tables.rinkmena.select().where(Tables.rinkmena.c.ID == 1)
    opendatagov_database = dict()
    for row in con.execute(clause):
        for row_name in row.keys():
            opendatagov_database[row_name] = row[row_name]
    with mock.patch('ckanext.harvest.harvesters.\
ckanharvester.config') as config:
        config.__getitem__.side_effect = db_url.split()
        results_by_guid = run_harvest(
            url='http://localhost:%s/' % mock_ckan.PORT,
            harvester=CKANHarvester())
    result = results_by_guid['1']
    assert result['state'] == 'COMPLETE'
    assert result['report_status'] == 'added'
    assert result['errors'] == []
    result = results_by_guid['2']
    assert result['state'] == 'COMPLETE'
    assert result['report_status'] == 'added'
    assert result['errors'] == []
    assert was_last_job_considered_error_free()
