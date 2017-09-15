 # -*- coding: utf-8 -*-
import urllib2
import httplib
import datetime
import socket
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import Table, MetaData
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError
import logging

log = logging.getLogger(__name__)

from base import HarvesterBase

#harvesteris parašytas pakeitus ckan-harvester kodą
#kol kas harvestina viską kas yra duombazėj.
#kadangi pas mane nesukonfiguruotas cron tai pats harvesteris automatiškai nepasileidžia
#norint ckan privesti paleisti harvesti reik rašyti šias komandas esant opendata.gov.lt/ckan
#paster --plugin=ckanext-harvest harvester gather_consumer vienam terminalo lange ir
#paster --plugin=ckanext-harvest harvester fetch_consumer kitam terminalo lange

class CKANHarvester(HarvesterBase):

    def _connect(self, user, password, db, host='192.168.56.101'): #host: duombazės ip
    	url = 'mysql://{}:{}@{}/{}'
    	url = url.format(user, password, host, db)
   	con = create_engine(url)
  	meta = MetaData(bind=con, reflect=True)
   	return con, meta

    def info(self):
        return {
            'name': 'opendatagov',
            'title': 'opendata.gov.lt',
            'description': 'Harvest opendata.gov.lt',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In opendatagov gather_stage')
        con, meta = self._connect('root', 'root', 'opendatagov') #root: laukeliuose jūsų duombazės user ir pass, o
	results = meta.tables['t_rinkmena']			 #opendatagov: laukelyje duombazės pavadinimas
	clause = results.select()
	ids = []
	for row in con.execute(clause):
	    id = row[0]
	    obj = HarvestObject(guid=id, job=harvest_job, content='%s,%s' % (row[0], row[2]))
            obj.save()
            ids.append(obj.id)
	return ids
    
    def fetch_stage(self, harvest_object):
	return True

    def import_stage(self,harvest_object):
	data_to_import = harvest_object.content.split(',')
	package_dict = {
		'id' : data_to_import[0],
		'title' : data_to_import[1],
		'owner_org' : 'orga'
	}
        return self._create_or_update_package(package_dict, harvest_object)
