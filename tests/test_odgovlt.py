# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import contextlib
import gettext
import json
import logging
import os

import mock
import pkg_resources as pres
import psycopg2
import psycopg2.extensions
import pylons
import pytest
import sqlalchemy as sa
import webtest

import ckan.config.middleware
import ckan.lib.search.index
import ckan.model
import ckanext.harvest.model
from ckan.tests.helpers import reset_db
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.tests.factories import HarvestJobObj
from ckanext.harvest.tests.factories import HarvestObjectObj
from ckanext.harvest.tests.factories import HarvestSourceObj
from ckanext.harvest.tests.lib import run_harvest

from odgovlt import CkanAPI
from odgovlt import DatetimeEncoder
from odgovlt import OdgovltHarvester
from odgovlt import fixcase
from odgovlt import get_package_tags
from odgovlt import slugify
from odgovlt import IvpkIrsSync


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
    last_job = (
        ckan.model.Session.
        query(ckanext.harvest.model.HarvestJob).
        order_by(ckanext.harvest.model.HarvestJob.created.desc()).
        first()
    )
    job = mock.MagicMock()
    job.source = last_job.source
    job.id = ''
    return bool(CKANHarvester.last_error_free_job(job))


@pytest.fixture
def db():
    engine = sa.create_engine('sqlite://')

    with open(os.path.join(os.path.dirname(__file__), 'schema.sql')) as f:
        engine.raw_connection().executescript(f.read())

    return engine


@pytest.fixture
def postgres():
    dbname = 'odgovlt_mysql_import_tests'
    with contextlib.closing(psycopg2.connect('postgresql:///postgres')) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with contextlib.closing(conn.cursor()) as curs:
            curs.execute('DROP DATABASE IF EXISTS ' + dbname)
            curs.execute('CREATE DATABASE ' + dbname)
    return 'postgresql:///%s' % dbname


@pytest.fixture
def app(postgres, mocker, caplog):
    caplog.set_level(logging.WARNING, logger='ckan.lib.i18n')
    caplog.set_level(logging.WARNING, logger='migrate')
    caplog.set_level(logging.WARNING, logger='pyutilib')
    caplog.set_level(logging.WARNING, logger='vdm')
    caplog.set_level(logging.WARNING, logger='pysolr')

    global_config = {
        '__file__': '',
        'here': os.path.dirname(__file__),
        'ckan.site_url': 'http://localhost',
        'ckan.plugins': 'harvest odgovlt_harvester',
        'sqlalchemy.url': postgres,
        'ckanext.harvest.user_name': 'harvest',
        # 'solr_url': 'http://127.0.0.1:8983/solr/ckan',
    }
    app_config = {
        'who.config_file': pres.resource_filename('ckan.config', 'who.ini'),
        'beaker.session.secret': 'secret',
    }
    app = ckan.config.middleware.make_app(global_config, **app_config)
    app = CKANTestApp(app)

    ckan.model.repo.init_db()
    ckanext.harvest.model.setup()

    pylons.translator = gettext.NullTranslations()

    return app


def test_OdgovltHarvester(app, db, mocker):
    sync = IvpkIrsSync(db)
    mocker.patch('odgovlt.IvpkIrsSync', return_value=sync)

    db.execute(sync.t.rinkmena.insert(), {
        'PAVADINIMAS': 'Testinė rinkmena nr. 1',
        'SANTRAUKA': 'Testas nr. 1',
        'TINKLAPIS': 'http://www.testas1.lt',
        'R_ZODZIAI': '​Šilumos tiekimo licencijas turinčių įmonių sąrašas,'
                     'šiluma,'
                     'šilumos tiekėjai,'
                     'licencijos,'
                     'licencijuojamos veiklos teritorija',
        'K_EMAIL': 'testas1@testas1.com',
        'STATUSAS': 'U',
        'USER_ID': 1,
        'istaiga_id': 1,
    })

    db.execute(sync.t.rinkmena.insert(), {
        'PAVADINIMAS': 'Testinė rinkmena nr. 2',
        'SANTRAUKA': 'Testas nr. 2',
        'TINKLAPIS': 'http://www.testas2.lt',
        'R_ZODZIAI': 'keliai,eismo intensyvumas,"e"',
        'K_EMAIL': 'testas2@testas2.com',
        'STATUSAS': 'U',
        'USER_ID': 2,
        'istaiga_id': 2,
    })

    db.execute(sync.t.user.insert(), {
        'LOGIN': 'User1',
        'PASS': 'secret123',
        'EMAIL': 'testas1@testas1.com',
        'TELEFONAS': '+37000000000',
        'FIRST_NAME': 'Jonas',
        'LAST_NAME': 'Jonaitis',
    })

    db.execute(sync.t.user.insert(), {
        'LOGIN': 'User2',
        'PASS': 'secret123',
        'EMAIL': 'testas2@testas2.com',
        'TELEFONAS': '+37000000000',
        'FIRST_NAME': 'Tomas',
        'LAST_NAME': 'Tomauskas',
    })

    db.execute(sync.t.istaiga.insert(), {
        'PAVADINIMAS': 'Testinė organizacija nr. 1',
        'KODAS': 888,
        'ADRESAS': 'Testinė g. 9'
    })

    db.execute(sync.t.istaiga.insert(), {
        'PAVADINIMAS': 'Testinė organizacija nr. 2',
        'KODAS': 777,
        'ADRESAS': 'Testinė g. 91'
    })

    db.execute(sync.t.kategorija.insert(), {
        'PAVADINIMAS': 'testas1',
        'KATEGORIJA_ID': 0,
        'LYGIS': 1
    })

    db.execute(sync.t.kategorija.insert(), {
        'PAVADINIMAS': 'testas2',
        'KATEGORIJA_ID': 0,
        'LYGIS': 1
    })

    db.execute(sync.t.kategorija.insert(), {
        'PAVADINIMAS': 'testas3',
        'KATEGORIJA_ID': 1,
        'LYGIS': 2
    })

    db.execute(sync.t.kategorija.insert(), {
        'PAVADINIMAS': 'testas4',
        'KATEGORIJA_ID': 2,
        'LYGIS': 2
    })

    db.execute(sync.t.kategorija.insert(), {
        'PAVADINIMAS': 'testas5',
        'KATEGORIJA_ID': 3,
        'LYGIS': 3
    })

    db.execute(sync.t.kategorija.insert(), {
        'PAVADINIMAS': 'testas6',
        'KATEGORIJA_ID': 4,
        'LYGIS': 3
    })

    db.execute(sync.t.kategorija.insert(), {
        'PAVADINIMAS': 'testas7',
        'KATEGORIJA_ID': 4,
        'LYGIS': 3
    })

    db.execute(sync.t.kategorija_rinkmena.insert(), {
        'KATEGORIJA_ID': 1,
        'RINKMENA_ID': 1
    })

    db.execute(sync.t.kategorija_rinkmena.insert(), {
        'KATEGORIJA_ID': 3,
        'RINKMENA_ID': 2
    })

    ckanapi = CkanAPI({'user': 'harvest'})

    source = HarvestSourceObj(url='sqlite://', source_type='opendata-gov-lt')
    job = HarvestJobObj(source=source)
    harvester = OdgovltHarvester()

    obj_ids = harvester.gather_stage(job)
    assert job.gather_errors == []

    assert ckanapi.group_list() == [
        'testas1-1',
        'testas2-2',
        'testas3-3',
        'testas4-4',
        'testas5-5',
        'testas6-6',
        'testas7-7',
    ]

    def subgroups(name):
        return [g['name'] for g in ckanapi.group_show(id=name)['groups']]

    assert subgroups('testas1-1') == ['testas3-3']
    assert subgroups('testas2-2') == ['testas4-4']
    assert subgroups('testas3-3') == ['testas5-5']
    assert subgroups('testas4-4') == ['testas6-6', 'testas7-7']
    assert subgroups('testas5-5') == []
    assert subgroups('testas6-6') == []
    assert subgroups('testas7-7') == []

    assert [json.loads(ckanext.harvest.model.HarvestObject.get(x).content)['PAVADINIMAS'] for x in obj_ids] == [
        'Testinė rinkmena nr. 1',
        'Testinė rinkmena nr. 2',
    ]
    database_data_list = [dict(row) for row in db.execute(sync.t.rinkmena.select())]
    user1 = sync.sync_user(1)
    user2 = sync.sync_user(2)
    user3 = sync.sync_user(3)
    database_data_list[0]['VARDAS'] = user1['fullname']
    database_data_list[1]['VARDAS'] = user2['fullname']
    organization1 = sync.sync_organization(1)
    organization2 = sync.sync_organization(2)
    organization3 = sync.sync_organization(3)
    database_data_list[0]['ORGANIZACIJA'] = organization1['name']
    database_data_list[1]['ORGANIZACIJA'] = organization2['name']
    database_data_list[0]['KATEGORIJA_RINKMENA'] = json.dumps([
        {
            'ID': 1,
            'KATEGORIJA_ID': 1,
            'RINKMENA_ID': 1,
        }
    ])
    database_data_list[1]['KATEGORIJA_RINKMENA'] = json.dumps([
        {
            'ID': 2,
            'KATEGORIJA_ID': 1,
            'RINKMENA_ID': 2,
        }
    ])
    obj1 = HarvestObjectObj(
        guid=database_data_list[0]['ID'],
        job=job,
        content=json.dumps(database_data_list[0], cls=DatetimeEncoder))
    result = harvester.fetch_stage(obj1)
    assert obj1.errors == []
    assert result
    obj2 = HarvestObjectObj(
        guid=database_data_list[1]['ID'],
        job=job,
        content=json.dumps(database_data_list[1], cls=DatetimeEncoder))
    result = harvester.fetch_stage(obj2)
    assert obj2.errors == []
    assert result
    create_or_update = harvester.import_stage(obj1)
    assert create_or_update
    create_or_update = harvester.import_stage(obj2)
    assert create_or_update
    assert obj1.package_id
    assert obj2.package_id
    reset_db()
    sync = IvpkIrsSync(db)
    mocker.patch('odgovlt.IvpkIrsSync', return_value=sync)
    results_by_guid = run_harvest(url='sqlite://', harvester=OdgovltHarvester())
    result = results_by_guid['1']
    assert result['state'] == 'COMPLETE'
    assert result['report_status'] == 'added'
    assert result['errors'] == []
    result = results_by_guid['2']
    assert result['state'] == 'COMPLETE'
    assert result['report_status'] == 'added'
    assert result['errors'] == []
    assert was_last_job_considered_error_free()
    ids = ckanapi.package_list()
    assert len(ids) == 3
    package1 = ckanapi.package_show(id=ids[0])
    package2 = ckanapi.package_show(id=ids[1])
    assert package1['title'] == 'Testinė rinkmena nr. 1'
    assert package1['notes'] == 'Testas nr. 1'
    assert package1['url'] == 'http://www.testas1.lt'
    assert package1['maintainer'] == 'Jonas Jonaitis'
    assert package1['maintainer_email'] == 'testas1@testas1.com'
    assert package1['organization']['title'] == 'Testinė organizacija nr. 1'
    assert package1['groups'] == [
        {
            'display_name': 'testas1',
            'description': '',
            'image_display_url': '',
            'title': 'testas1',
            'id': package1['groups'][0]['id'],
            'name': 'testas1-1',
        }
    ]
    assert package2['title'] == 'Testinė rinkmena nr. 2'
    assert package2['notes'] == 'Testas nr. 2'
    assert package2['url'] == 'http://www.testas2.lt'
    assert package2['maintainer'] == 'Tomas Tomauskas'
    assert package2['maintainer_email'] == 'testas2@testas2.com'
    assert package2['organization']['title'] == 'Testinė organizacija nr. 2'
    assert package2['groups'] == [
        {
            'display_name': 'testas3',
            'description': '',
            'image_display_url': '',
            'title': 'testas3',
            'id': package2['groups'][0]['id'],
            'name': 'testas3-3',
        }
    ]
    assert user3['fullname'] == 'Unknown User'
    assert organization3['title'] == 'Unknown organization'
    fixcase_test = fixcase('Testas9')
    assert fixcase_test == 'testas9'
    tags_test = get_package_tags(
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,'
        'testas2 testas3, testas4 testas5; testas6'
    )
    assert tags_test == [
        'testas2 testas3',
        'testas4 testas5',
        'testas6',
    ]
    tags1 = package1['tags']
    tags2 = package2['tags']
    assert sorted([x['name'] for x in tags1]) == [
        'licencijos',
        'licencijuojamos veiklos teritorija',
        'šiluma',
        'šilumos tiekimo licencijas turinčių įmonių sąrašas',
        'šilumos tiekėjai',
    ]
    assert sorted([x['name'] for x in tags2]) == [
        'eismo intensyvumas',
        'keliai',
    ]


def test_slugify():
    title = (
        'Radiacinės saugos centro išduotų galiojančių '
        'licencijų verstis veikla su jonizuojančiosios spinduliuotės '
        'šaltiniais duomenys'
    )
    assert len(slugify(title, length=42)) < 42
    assert slugify(title, length=42) == 'radiacines-saugos--duomenys-saltiniais'
    assert slugify() == ''
