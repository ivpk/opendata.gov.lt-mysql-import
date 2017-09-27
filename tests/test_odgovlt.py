from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.harvesters.ckanharvester import DatetimeEncoder
from sqlalchemy import MetaData
from sqlalchemy.engine.base import Engine
from datetime import datetime


def test_info():
    ckan_object = CKANHarvester()
    info = ckan_object.info()
    assert isinstance(info, dict)
    assert isinstance(info['name'], str)
    assert isinstance(info['title'], str)
    assert isinstance(info['description'], str)
    assert isinstance(info['form_config_interface'], str)


def test_connect_to_database(mocker):
    ckan_object = CKANHarvester()
    config = mocker.patch('ckanext.harvest.harvesters.ckanharvester.config')
    config.__getitem__.side_effect = 'mysql://test'.split()
    mocker.patch('ckanext.harvest.harvesters.ckanharvester.MetaData')
    con, meta = ckan_object._connect_to_database()
    assert isinstance(con, Engine)
    mocker.stopall()
    config = mocker.patch('ckanext.harvest.harvesters.ckanharvester.config')
    config.__getitem__.side_effect = 'mysql://test'.split()
    mocker.patch('ckanext.harvest.harvesters.ckanharvester.create_engine')
    con, meta = ckan_object._connect_to_database()
    assert isinstance(meta, MetaData)


def test_DatetimeEncoder(mocker):
    datetime_object = DatetimeEncoder()
    mocker.patch('ckanext.harvest.harvesters.ckanharvester.json.JSONEncoder')
    mocker.patch('ckanext.harvest.harvesters.ckanharvester.super')
    date_now = datetime.now()
    date_1 = date_now.strftime('%Y-%m-%dT%H:%M:%S')
    date_2 = datetime_object.default(date_now)
    assert isinstance(date_2, str)
    assert date_1 == date_2


def test_fetch_stage(mocker):
    ckan_object = CKANHarvester()
    HarvestObject = mocker.patch('ckanext.harvest.harvesters.\
ckanharvester.HarvestObject')
    assert ckan_object.fetch_stage(HarvestObject)
