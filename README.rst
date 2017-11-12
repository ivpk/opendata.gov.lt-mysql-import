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
