from setuptools import setup, find_packages


setup(
    name='odgovlt-mysql-import',
    version='0.1',
    license='AGPL',
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        'paste.paster_command': [
            'odgovltsync=odgovltimport:OpenDataGovLtCommand',
        ]
    },
    install_requires=[
        'Unidecode',
        'sqlalchemy',
        'PyMySQL',
    ],
    extras_require={
        'tests': [
            'pytest',
            'pytest-catchlog',
        ]
    },
)
