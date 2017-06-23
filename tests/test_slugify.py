# coding: utf-8

from __future__ import unicode_literals

from odgovltimport import slugify


def test_slugify():
    title = (
        'Radiacinės saugos centro išduotų galiojančių licencijų verstis veikla su jonizuojančiosios spinduliuotės '
        'šaltiniais duomenys'
    )
    slug = slugify(title, length=42)
    assert len(slug) <= 42
    assert slug == 'radiacines-saugos--duomenys-saltiniais'
