from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.harvesters.ckanharvester import DatetimeEncoder
from mock import patch
from sqlalchemy import MetaData
from sqlalchemy.engine.base import Engine
from datetime import datetime

ckan_object = CKANHarvester()
datetime_object = DatetimeEncoder()


def test_info():
    info = ckan_object.info()
    assert isinstance(info, dict)
    dict_names = [
           'name',
           'title',
           'description',
           'form_config_interface']
    for dict_name in info:
        assert isinstance(info[dict_name], str)
        assert isinstance(dict_name, str)
        assert dict_name in dict_names
    assert info['form_config_interface'] == 'Text'


def test_connect_to_database():
    @patch('ckanext.harvest.harvesters.ckanharvester.create_engine')
    @patch('ckanext.harvest.harvesters.ckanharvester.config')
    def test_meta(config_mock, create_engine_mock):
        config = dict()
        config['odgovltimport.externaldb.url'] = 'mysql://test'
        config_mock.__getitem__.side_effect = config.__getitem__
        con, meta = ckan_object._connect_to_database()
        assert isinstance(meta, MetaData)

    @patch('ckanext.harvest.harvesters.ckanharvester.MetaData')
    @patch('ckanext.harvest.harvesters.ckanharvester.config')
    def test_con(config_mock, MetaData_mock):
        config = dict()
        config['odgovltimport.externaldb.url'] = 'mysql://test'
        config_mock.__getitem__.side_effect = config.__getitem__
        con, meta = ckan_object._connect_to_database()
        assert isinstance(con, Engine)

    test_meta()
    test_con()


@patch('ckanext.harvest.harvesters.ckanharvester.json.JSONEncoder')
@patch('ckanext.harvest.harvesters.ckanharvester.super')
def test_DatetimeEncoder(json_mock, super_mock):
    date_now = datetime.now()
    date_1 = date_now.strftime('%Y-%m-%dT%H:%M:%S')
    date_2 = datetime_object.default(date_now)
    assert isinstance(date_2, str)
    assert date_1 == date_2
