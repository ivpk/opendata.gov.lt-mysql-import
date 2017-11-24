.. image:: https://travis-ci.org/ivpk/opendata.gov.lt-mysql-import.svg?branch=master
    :target: https://travis-ci.org/ivpk/opendata.gov.lt-mysql-import


Harvester for opendata.gov.lt old database. This harvester connects directly to
MySQL database of old opendata.gov.lt website and harvestes datasets from there
to CKAN.

To install CKAN plugin you need to add it to CKAN configuration::

    ckan.plugins =
        harvest
        odgovlt_harvester


Connection parameters to MySQL database should be specified as harvester URL,
and it should looks like this::

    mysql+pymysql://usernaem:password@hostname/dbname?charset=utf8


Development environment
=======================

Initialize deveopment environment::

    make

You can run SOLR like this::

    sudo docker-compose up -d


Testing the harvester
---------------------

In order to test if harvester works, first you need to create harvester source,
you can do this using CKAN admin interface:

http://127.0.0.1:5000/harvest

Once you have the harvester set up and read you can get harvester source id
using this command::

    paster --plugin=ckanext-harvest harvester sources -c development.ini                                      

Then you can run harvester using this command::

    paster --plugin=ckanext-harvest harvester run_test <source-id> -c development.ini


Accessing CKAN API from IPython
-------------------------------

.. code-block:: python

    # Load CKAN configuration
    from ckan.lib.cli import CkanCommand
    command = CkanCommand('')
    command.options = type('Args', (), {'config': 'development.ini'})
    command._load_config()

    # Create CKAN API instance
    from ckanapi import LocalCKAN
    api = LocalCKAN()

    # Access CKAN API
    packages = api.action.package_list()
    package = api.action.package_show(name_or_id=packages[0])

Deleting all groups:

.. code-block:: python

    for name in api.action.group_list():
        api.action.group_delete(id=name)
