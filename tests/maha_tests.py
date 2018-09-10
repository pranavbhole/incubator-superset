"""Unit tests for maha features"""

from superset.connectors.maha.models import MahaRegistry, CubeMetric, MahaColumn, Cube
from superset.models.core import Slice,MahaApi, Lookback
from datetime import datetime, timedelta
from mock import Mock, patch
import httpretty
import unittest
import json
import superset
from superset import app, db, models, utils, appbuilder, get_session
from flask import request, flash
import tempfile
import mock
import calendar, time
import os

class MahaTest(unittest.TestCase):

    """Testing interactions with maha registries"""

    def __init__(self, *args, **kwargs):
        super(MahaTest, self).__init__(*args, **kwargs)

    def setUp(self):
        session = get_session()
        httpretty.enable()
        self.clearDBData()
        domain_json = json.dumps({"schemas": {"caravel": ["test_supply_stats_cube", "test_cube2"]}})
        httpretty.register_uri(httpretty.GET,
                               "http://test.maha.api.com:80/na_maha_cb_ws/external/api/v2/async/flattenDomain/",
                               domain_json)

        registry = (
            session
            .query(MahaRegistry)
            .filter_by(registry_name="test_registry")
            .first()
        )

        if registry:
            session.delete(registry)
            session.commit()

        registry = MahaRegistry(
            registry_name='test_registry',
            registry_host='test.maha.api.com',
            registry_port='80',
            registry_endpoint='na_maha_cb_ws/external/api/v2',
            domain_endpoint = 'async/flattenDomain',
            sync_request_endpoint = 'schemas/caravel/irsync',
            metadata_last_refreshed=datetime.now()
        )


        test_supply_stats_json = json.dumps({
            "name": "test_supply_stats",
            "mainEntityIds": {
                "publisher": "Publisher ID",
                "publisher_ll": "Publisher ID"
            },
            "maxDaysLookBack": [
                {
                    "requestType": "SyncRequest",
                    "grain": "DailyGrain",
                    "days": 30
                },
                {
                    "requestType": "AsyncRequest",
                    "grain": "DailyGrain",
                    "days": 30
                }
            ],
            "maxDaysWindow": [
                {
                    "requestType": "SyncRequest",
                    "grain": "DailyGrain",
                    "days": 20
                },
                {
                    "requestType": "AsyncRequest",
                    "grain": "DailyGrain",
                    "days": 20
                },
                {
                    "requestType": "SyncRequest",
                    "grain": "HourlyGrain",
                    "days": 20
                },
                {
                    "requestType": "AsyncRequest",
                    "grain": "HourlyGrain",
                    "days": 20
                }
            ],
            "fields": [
                {
                    "field": "Test Country ISO Code",
                    "type": "Dimension",
                    "dataType": {
                        "type": "String",
                        "constraint": "null"
                    },
                    "dimensionName": "null",
                    "filterable": 1,
                    "filterOperations": [
                        "IN"
                    ],
                    "required": "false",
                    "filteringRequired": "false"
                },
                {
                    "field": "Test Day",
                    "type": "Dimension",
                    "dataType": {
                        "type": "Date",
                        "constraint": "YYYY-MM-dd"
                    },
                    "dimensionName": "null",
                    "filterable": 1,
                    "filterOperations": [
                        "BETWEEN",
                        "="
                    ],
                    "required": "false",
                    "filteringRequired": "true"
                },
                {
                    "field": "Test Fact",
                    "type": "Fact",
                    "dataType": {
                        "type": "String",
                        "constraint": "null"
                    },
                    "dimensionName": "null",
                    "filterable": 1,
                    "filterOperations": [
                        "BETWEEN",
                        "="
                    ],
                    "required": "false",
                    "filteringRequired": "true"
                }

            ]

        })

        test_cube2_json = json.dumps({
            "name": "test_cube2",
            "mainEntityIds": {
                "publisher": "Publisher ID",
                "publisher_ll": "Publisher ID"
            },
            "maxDaysLookBack": [
            ],
            "maxDaysWindow": [
            ],
            "fields": [
                {
                    "field": "Test Country ISO Code",
                    "type": "Dimension",
                    "dataType": {
                        "type": "String",
                        "constraint": "null"
                    },
                    "dimensionName": "null",
                    "filterable": 1,
                    "filterOperations": [
                        "IN"
                    ],
                    "required": "false",
                    "filteringRequired": "false"
                },
                {
                    "field": "Test Day",
                    "type": "Dimension",
                    "dataType": {
                        "type": "Date",
                        "constraint": "YYYY-MM-dd"
                    },
                    "dimensionName": "null",
                    "filterable": 1,
                    "filterOperations": [
                        "BETWEEN",
                        "="
                    ],
                    "required": "false",
                    "filteringRequired": "true"
                },
                {
                    "field": "Test Fact",
                    "type": "Fact",
                    "dataType": {
                        "type": "String",
                        "constraint": "null"
                    },
                    "dimensionName": "null",
                    "filterable": 1,
                    "filterOperations": [
                        "BETWEEN",
                        "="
                    ],
                    "required": "false",
                    "filteringRequired": "true"
                }

            ]

        })
        httpretty.register_uri(httpretty.GET,
                               "http://test.maha.api.com:80/na_maha_cb_ws"
                               "/external/api/v2/async/flattenDomain/cubes/test_supply_stats_cube/0",
                               test_supply_stats_json)

        httpretty.register_uri(httpretty.GET,
                               "http://test.maha.api.com:80/na_maha_cb_ws"
                               "/external/api/v2/async/flattenDomain/cubes/test_cube2/0",
                               test_cube2_json)


        session.add(registry)
        registry.refresh_datasources()

        sample_response = "{\"header\":{\"cube\":\"keyword_stats\",\"fields\":[{\"fieldName\":\"Keyword ID\",\"fieldType\":\"DIM\"},{\"fieldName\":\"Day\",\"fieldType\":\"DIM\"},{\"fieldName\":\"Keyword Value\",\"fieldType\":\"CONSTANT\"},{\"fieldName\":\"Keyword Status\",\"fieldType\":\"CONSTANT\"},{\"fieldName\":\"CPC\",\"fieldType\":\"CONSTANT\"},{\"fieldName\":\"Keyword Match Type\",\"fieldType\":\"CONSTANT\"},{\"fieldName\":\"CTR\",\"fieldType\":\"FACT\"},{\"fieldName\":\"Post Click Conversions\",\"fieldType\":\"FACT\"},{\"fieldName\":\"Keyword Match Type Full\",\"fieldType\":\"CONSTANT\"},{\"fieldName\":\"Impressions\",\"fieldType\":\"FACT\"},{\"fieldName\":\"Clicks\",\"fieldType\":\"FACT\"},{\"fieldName\":\"Spend\",\"fieldType\":\"FACT\"}],\"maxRows\":200},\"rows\":[[295432066051,null,\"platforms sandals women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066052,null,\"womens high wedge sandals\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066053,null,\"chunky sandals for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066054,null,\"womens black wedge sandals\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066055,null,\"womens platform sandals\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066056,null,\"womens platform sandals wedge\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066057,null,\"low wedge sandal for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[\"295432066058\",\"20160708\",\"wedge sandals for women\",\"ON\",1.0,\"PHRASE\",0.0,0,\"PHRASE\",2,0,0.0],[295432066059,null,\"chunky sandals women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066060,null,\"sandals wedges women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066061,null,\"wedge dress sandals women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066062,null,\"womens sandals platform\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066063,null,\"platform sandals wedge for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066064,null,\"womens black wedge sandal\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066065,null,\"sandals wedge for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066066,null,\"sandals platform women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066067,null,\"womens sandal platform\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066068,null,\"womens platform sandal\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066069,null,\"black wedge sandals women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066070,null,\"platform sandal for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066071,null,\"high wedge sandals women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[\"295432066072\",\"20160708\",\"womens wedge sandals\",\"ON\",1.0,\"PHRASE\",0.0,0,\"PHRASE\",1,0,0.0],[295432066073,null,\"black wedge sandal women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066074,null,\"high wedge sandals for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066075,null,\"low wedge sandal women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066076,null,\"platform sandals women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066077,null,\"sandal wedges women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066078,null,\"wedge sandals women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066079,null,\"sandal platform for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066080,null,\"sandals wedge women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[\"295432066081\",\"20160706\",\"sandals wedges for women\",\"ON\",1.0,\"PHRASE\",0.0,0,\"PHRASE\",1,0,0.0],[295432066082,null,\"womens sandals wedges\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066083,null,\"womens sandals wedge\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066084,null,\"womens wedge dress sandals\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[\"295432066085\",\"20160708\",\"low wedge sandals for women\",\"ON\",1.0,\"PHRASE\",0.0,0,\"PHRASE\",2,0,0.0],[295432066086,null,\"platform sandals wedge women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066087,null,\"platform sandal women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066088,null,\"wedge dress sandals for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066089,null,\"womens chunky sandals\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066090,null,\"black wedge sandals for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066091,null,\"black wedge sandal for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066092,null,\"low wedge sandals women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066093,null,\"platform sandals for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066094,null,\"womens sandal wedges\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066095,null,\"womens low wedge sandals\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066096,null,\"sandals platform for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066097,null,\"platforms sandals for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[\"295432066098\",\"20160701\",\"womens platforms sandals\",\"ON\",1.0,\"PHRASE\",0.0,0,\"PHRASE\",1,0,0.0],[295432066099,null,\"sandal platform women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066100,null,\"womens low wedge sandal\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066101,null,\"sandal wedges for women\",\"ON\",1.0,\"PHRASE\",null,null,\"PHRASE\",null,null,null],[295432066102,null,\"womens black wedge sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066103,null,\"womens platform sandal\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066104,null,\"sandals wedges women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066105,null,\"womens wedge dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066106,null,\"wedge sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066107,null,\"womens high wedge sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066108,null,\"womens chunky sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[\"295432066109\",\"20160705\",\"womens platforms sandals\",\"ON\",1.5,\"EXACT\",0.0,0,\"EXACT\",1,0,0.0],[295432066110,null,\"low wedge sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[\"295432066111\",\"20160703\",\"platform sandal women\",\"ON\",1.5,\"EXACT\",0.0,0,\"EXACT\",1,0,0.0],[295432066112,null,\"platform sandals wedge for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066113,null,\"womens black wedge sandal\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066114,null,\"womens low wedge sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066115,null,\"womens sandals platform\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066116,null,\"chunky sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066117,null,\"platform sandal for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066118,null,\"wedge dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066119,null,\"sandals wedge for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066120,null,\"platforms sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066121,null,\"womens sandal platform\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066122,null,\"sandals platform women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066123,null,\"sandal platform for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066124,null,\"low wedge sandal for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066125,null,\"chunky sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066126,null,\"black wedge sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066127,null,\"high wedge sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066128,null,\"sandal wedges women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066129,null,\"platform sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066130,null,\"black wedge sandal for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066131,null,\"wedge dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066132,null,\"womens sandals wedges\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066133,null,\"sandal wedges for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066134,null,\"sandals wedges for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066135,null,\"womens sandals wedge\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066136,null,\"sandals wedge women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066137,null,\"platform sandals wedge women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066138,null,\"womens low wedge sandal\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066139,null,\"low wedge sandal women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[\"295432066140\",\"20160708\",\"womens wedge sandals\",\"ON\",1.5,\"EXACT\",0.0,0,\"EXACT\",1,0,0.0],[295432066141,null,\"black wedge sandal women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[\"295432066142\",\"20160706\",\"black wedge sandals women\",\"ON\",1.5,\"EXACT\",0.0,0,\"EXACT\",1,0,0.0],[295432066143,null,\"womens sandal wedges\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066144,null,\"low wedge sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066145,null,\"platform sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066146,null,\"womens platform sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066147,null,\"sandals platform for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066148,null,\"high wedge sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[\"295432066149\",\"20160706\",\"wedge sandals for women\",\"ON\",1.25,\"EXACT\",0.5,0,\"EXACT\",2,1,1.0399999618530273],[\"295432066150\",\"20160708\",\"platforms sandals for women\",\"ON\",1.5,\"EXACT\",0.5,0,\"EXACT\",2,1,0.9300000071525574],[295432066151,null,\"womens platform sandals wedge\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066152,null,\"sandal platform women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066153,null,\"womens grey dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066154,null,\"womens rhinestone dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066155,null,\"navy dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066156,null,\"womens beaded dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066157,null,\"pewter dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066158,null,\"womens dress thong sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066159,null,\"brown dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066160,null,\"womens dress sandals low heel\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066161,null,\"gray dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066162,null,\"black dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066163,null,\"black dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066164,null,\"womens dress sandal\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066165,null,\"embellished dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066166,null,\"womens beige dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066167,null,\"womens wide dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066168,null,\"gold dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066169,null,\"womens clear dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066170,null,\"womens casual dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066171,null,\"navy dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066172,null,\"dress sandal women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066173,null,\"tan dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066174,null,\"womens discount dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066175,null,\"womens platform dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066176,null,\"womens copper dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066177,null,\"casual dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066178,null,\"dress sandals low heel women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066179,null,\"silver dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[\"295432066180\",\"20160705\",\"gold dress sandals for women\",\"ON\",1.5,\"EXACT\",0.0,0,\"EXACT\",1,0,0.0],[295432066181,null,\"beaded dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[\"295432066182\",\"20160705\",\"womens white dress sandals\",\"ON\",1.5,\"EXACT\",0.0,0,\"EXACT\",2,0,0.0],[295432066183,null,\"womens embellished dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066184,null,\"white dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066185,null,\"womens ivory dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066186,null,\"rhinestone dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066187,null,\"clear dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066188,null,\"womens gold dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066189,null,\"embellished dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066190,null,\"copper dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066191,null,\"summer dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066192,null,\"cheap dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066193,null,\"white dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066194,null,\"dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066195,null,\"grey dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066196,null,\"discount dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[\"295432066197\",\"20160704\",\"dress sandal for women\",\"ON\",1.5,\"EXACT\",0.3333333333333333,0,\"EXACT\",3,1,1.3300000429153442],[295432066198,null,\"womens blue dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066199,null,\"beige dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066200,null,\"red dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066201,null,\"silver dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066202,null,\"womens dress sandals on sale\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066203,null,\"wide width dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066204,null,\"dress sandals low heel for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066205,null,\"pink dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066206,null,\"womens brown dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066207,null,\"tan dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066208,null,\"green dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066209,null,\"platform dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066210,null,\"womens tan dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066211,null,\"rhinestone dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066212,null,\"copper dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066213,null,\"dressy sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066214,null,\"casual dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066215,null,\"womens red dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066216,null,\"green dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066217,null,\"discount dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[\"295432066218\",\"20160703\",\"womens dress sandals\",\"ON\",1.5,\"EXACT\",0.0,0,\"EXACT\",1,0,0.0],[295432066219,null,\"womens pink dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066220,null,\"orange dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066221,null,\"beige dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066222,null,\"ivory dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066223,null,\"dress sandals for weddings for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066224,null,\"dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066225,null,\"dress sandals for weddings women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066226,null,\"multi color dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066227,null,\"gray dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066228,null,\"blue dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066229,null,\"womens gray dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066230,null,\"womens dress sandals for weddings\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066231,null,\"summer dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066232,null,\"dress thong sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066233,null,\"wide dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066234,null,\"dress sandals on sale women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066235,null,\"womens bronze dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066236,null,\"dress sandals on sale for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066237,null,\"cheap dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066238,null,\"platform dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066239,null,\"womens purple dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066240,null,\"brown dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066241,null,\"wide dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066242,null,\"womens summer dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066243,null,\"red dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066244,null,\"dressy sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066245,null,\"ivory dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[\"295432066246\",\"20160701\",\"womens silver dress sandals\",\"ON\",1.5,\"EXACT\",0.0,0,\"EXACT\",1,0,0.0],[295432066247,null,\"purple dress sandals women\",\"ON\",1.25,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066248,null,\"womens orange dress sandals\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066249,null,\"bronze dress sandals for women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null],[295432066250,null,\"clear dress sandals women\",\"ON\",1.5,\"EXACT\",null,null,\"EXACT\",null,null,null]]}"
        httpretty.register_uri(httpretty.POST, 
            "http://test.maha.api.com:80/na_maha_cb_ws/external/api/v2/schemas/caravel/irsync",
            sample_response)
        session.commit()
        
        
    def test_query_cube(self):
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_supply_stats_cube').first())
        print("Test Cube: {}".format(test_cube.cube_name))

        since = "7 days ago"
        from_dttm = utils.parse_human_datetime(since)
        until = "now"
        to_dttm = utils.parse_human_datetime(until)

        query_obj = {
            'granularity': 'year',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'is_timeseries': "True",
            'groupby': ['Keyword ID', 'Day', 'Keyword Value', 'Keyword Status', 'CPC', 'Keyword Match Type', 'CTR', 'Post Click Conversions', 'Keyword Match Type Full'],
            'metrics': ['Impressions', 'Clicks', 'Spend'],
            'row_limit': 200,
            'filter': [{'col': 'Section Name', 'val': ['0'], 'op': 'not in'}, {'col': 'Site Name', 'val': ['0'], 'op': 'not in'}],
            'timeseries_limit': 25,
            'extras': {},
        }

        with app.test_request_context():
            query_result = test_cube.query(query_obj)
            print(query_result.query)
    
    def test_get_query_str_granularity(self):
        print("==========Starting test_get_query_str_granularity==========")
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_supply_stats_cube').first())
        qry_start_dttm = datetime.now()
        from_dttm = datetime.now()
        to_dttm = datetime.now()
        groupby = []
        metrics = ['Impressions']
        query_obj = {
            'granularity': '1 day',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'groupby': groupby,
            'metrics': metrics,
            'extras': {},
        }
        query_str = test_cube.get_query_str("", qry_start_dttm, **query_obj)
        query_json = json.loads(query_str)
        assert({'field': 'Day'} in query_json['selectFields'])
        assert({'field': 'Hour'} not in query_json['selectFields'])
        query_obj2 = {
            'granularity': '1 hour',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'groupby': groupby,
            'metrics': metrics,
            'extras': {},
        }
        query_str2 = test_cube.get_query_str("", qry_start_dttm, **query_obj2)
        query_json2 = json.loads(query_str2)
        # For certain cubes, Hour col only contains month data. So we need Day col to get more accurate Hour info. 
        assert({'field': 'Day'} in query_json2['selectFields'])
        assert({'field': 'Hour'} in query_json2['selectFields'])
        query_obj3 = {
            'granularity': 'all',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'groupby': groupby,
            'metrics': metrics,
            'extras': {},
        }
        query_str3 = test_cube.get_query_str("", qry_start_dttm, **query_obj3)
        query_json3 = json.loads(query_str3)
        assert({'field': 'Day'} not in query_json3['selectFields'])
        assert({'field': 'Hour'} not in query_json3['selectFields'])
         
     
    def test_update_filter_values(self):
        print("==========Starting test_update_filter_values==========")
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_supply_stats_cube').first())
        ori_filter_vals1 = ['1234, 5678', '  987']
        updated_filter_vals1 = test_cube.update_filter_values(ori_filter_vals1)
        ori_filter_vals2 = []
        updated_filter_vals2 = test_cube.update_filter_values(ori_filter_vals2)
        ori_filter_vals3 = ['123', '456']
        updated_filter_vals3 = test_cube.update_filter_values(ori_filter_vals3)
        ori_filter_vals4_test_int = [123, 456]
        updated_filter_vals4_test_int = test_cube.update_filter_values(ori_filter_vals4_test_int)
        ori_filter_vals5_test_float = [123.45, 456.78]
        updated_filter_vals5_test_float = test_cube.update_filter_values(ori_filter_vals5_test_float)
        assert updated_filter_vals1 == ['1234', '5678', '987']
        assert updated_filter_vals2 == []
        assert updated_filter_vals3 == ['123', '456']
        assert updated_filter_vals4_test_int == [123, 456]
        assert updated_filter_vals5_test_float == [123.45, 456.78]
     
    def test_maha_api(self):
        myrid = calendar.timegm(time.gmtime())
        rid = MahaApi.get_request_id()
        assert rid >= myrid
 
        default_headers = MahaApi.get_default_headers(rid)
        assert default_headers['X-Request-Id'] == str(rid)
        assert default_headers['X-User-Id'] == 'superset'
        try:
            with app.test_request_context():
                sync_request_headers = MahaApi.get_sync_request_headers(request, rid)
                assert sync_request_headers['X-Request-Id'] == str(rid)
                assert sync_request_headers['Content-type'] == 'application/json'
                assert sync_request_headers['X-User-Id'] == 'superset'
        except Exception as e:
            self.fail("raised ExceptionType unexpectedly! ERROR: {}".format(e))
 
        session = get_session()
        test_registry = (
            session
                .query(MahaRegistry)
                .filter_by(registry_name="test_registry")
                .first()
        )
 
        domain_url = MahaApi.get_domain_url(test_registry)
        assert domain_url == 'http://test.maha.api.com:80/na_maha_cb_ws/external/api/v2/async/flattenDomain/'
        sync_url = MahaApi.get_sync_url(test_registry)
        assert sync_url == 'http://test.maha.api.com:80/na_maha_cb_ws/external/api/v2/schemas/caravel/irsync'
 
        test_cube = (
            session
                .query(Cube)
                .filter_by(cube_name="test_supply_stats_cube")
                .first()
        )
        cube_url = MahaApi.get_cube_url(test_registry, test_cube)
        assert cube_url == 'http://test.maha.api.com:80/na_maha_cb_ws/external/api/v2/async/flattenDomain/cubes/test_supply_stats_cube/0'
         
    def test_get_cubes(self):
        session = get_session()
        test_registry = (
            session
                .query(MahaRegistry)
                .filter_by(registry_name="test_registry")
                .first()
        )
        test_supply_stats_cube = (
            session
                .query(Cube)
                .filter_by(cube_name="test_supply_stats_cube")
                .first()
        )
        test_country_iso_code_col = (
            session
                .query(MahaColumn)
                .filter_by(column_name='Test Country ISO Code')
                .first()
        )
        test_day_col = (
            session
                .query(MahaColumn)
                .filter_by(column_name='Test Day')
                .first()
        )
 
        assert test_registry is not None
        assert test_supply_stats_cube is not None
        assert test_country_iso_code_col is not None
        assert test_day_col is not None
  
    def test_delete_lookback_days_when_cube_refresh(self):
        session = get_session()
        test_cube2 = session.query(Cube).filter_by(cube_name="test_cube2").first()
        # insert lookbacks for cube
        l = Lookback(cube_id=test_cube2.id, request_type="SyncRequest", grain="Day")
        session.add(l)
 
        l = session.query(Lookback).filter_by(cube_id=test_cube2.id).first()
        assert l is not None
        # refresh cube metadata
        test_registry = (
            session
            .query(MahaRegistry)
            .filter_by(registry_name="test_registry")
            .first()
        )
        test_registry.refresh_datasources()
 
        # shoule have empty lookback for cube
        l = session.query(Lookback).filter_by(cube_id=test_cube2.id).first()
        assert l is None
           
    def test_get_lookback_days(self):
        session = get_session()
        test_supply_stats_cube = session.query(Cube).filter_by(cube_name="test_supply_stats_cube").first()
         
        cube_id = test_supply_stats_cube.id
        test_lookback_sync_daily = session.query(Lookback).filter_by(
            cube_id=cube_id, request_type="SyncRequest", grain="DailyGrain").first()
        assert test_lookback_sync_daily is not None
        self.assertEqual(test_lookback_sync_daily.get_json, json.dumps({"request_type": "SyncRequest", "max_window": 20, "grain": "DailyGrain", "cube_id": cube_id, "lookback_days": 30}))
        self.assertEqual(test_lookback_sync_daily.max_window, 20)
        self.assertEqual(test_lookback_sync_daily.lookback_days, 30)
 
        test_lookback_async_daily = session.query(Lookback).filter_by(
            cube_id=cube_id, request_type="AsyncRequest", grain="DailyGrain").first()
        assert test_lookback_async_daily is not None
        self.assertEqual(test_lookback_async_daily.get_json, json.dumps({"request_type": "AsyncRequest", "max_window": 20, "grain": "DailyGrain", "cube_id": cube_id, "lookback_days": 30}))
        self.assertEqual(test_lookback_async_daily.max_window, 20)
        self.assertEqual(test_lookback_async_daily.lookback_days, 30)
 
        test_lookback_sync_all = session.query(Lookback).filter_by(
            cube_id=cube_id, request_type="SyncRequest", grain="All").first()
        assert test_lookback_sync_all is not None
        self.assertEqual(test_lookback_sync_all.get_json, json.dumps({"request_type": "SyncRequest", "max_window": 20, "grain": "All", "cube_id": cube_id, "lookback_days": 30}))
        self.assertEqual(test_lookback_sync_all.max_window, 20)
        self.assertEqual(test_lookback_sync_all.lookback_days, 30)
 
        test_lookback_async_all = session.query(Lookback).filter_by(
            cube_id=cube_id, request_type="AsyncRequest", grain="All").first()
        assert test_lookback_async_all is not None
        self.assertEqual(test_lookback_async_all.get_json, json.dumps({"request_type": "AsyncRequest", "max_window": 20, "grain": "All", "cube_id": cube_id, "lookback_days": 30}))
        self.assertEqual(test_lookback_async_all.max_window, 20)
        self.assertEqual(test_lookback_async_all.lookback_days, 30)
 
        test_lookback_sync_hourly = session.query(Lookback).filter_by(
            cube_id=cube_id, request_type="SyncRequest", grain="HourlyGrain").first()
        assert test_lookback_sync_hourly is not None
        self.assertEqual(test_lookback_sync_hourly.get_json, json.dumps({"request_type": "SyncRequest", "max_window": 20, "grain": "HourlyGrain", "cube_id": cube_id, "lookback_days": 400}))
        self.assertEqual(test_lookback_sync_hourly.max_window, 20)
        self.assertEqual(test_lookback_sync_hourly.lookback_days, 400)
 
        test_lookback_async_hourly = session.query(Lookback).filter_by(
            cube_id=cube_id, request_type="AsyncRequest", grain="HourlyGrain").first()
        assert test_lookback_async_hourly is not None
        self.assertEqual(test_lookback_async_hourly.get_json, json.dumps({"request_type": "AsyncRequest", "max_window": 20, "grain": "HourlyGrain", "cube_id": cube_id, "lookback_days": 400}))
        self.assertEqual(test_lookback_async_hourly.max_window, 20)
        self.assertEqual(test_lookback_async_hourly.lookback_days, 400)
 
        test_cube2 = session.query(Cube).filter_by(cube_name="test_cube2").first()
        test_lookback_empty = session.query(Lookback).filter_by(cube_id=test_cube2.id).first()
         
        assert test_lookback_empty is None
       
    def test_rendering_time_columns(self):
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_cube2').first())
 
        since = "2018-06-23"
        from_dttm = utils.parse_human_datetime(since)
        until = "2018-06-30"
        to_dttm = utils.parse_human_datetime(until)
 
        # with granularity all
        # no Day in group by
        # should contain no Day field
        query_obj = {
            'granularity': 'all',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'is_timeseries': "True",
            'groupby': ['Keyword ID', 'Keyword Value', 'Keyword Status', 'CPC', 'Keyword Match Type', 'CTR', 'Post Click Conversions', 'Keyword Match Type Full'],
            'metrics': ['Impressions', 'Clicks', 'Spend'],
            'row_limit': 200,
            'filter': [{'col': 'Section Name', 'val': ['0'], 'op': 'not in'}, {'col': 'Site Name', 'val': ['0'], 'op': 'not in'}],
            'timeseries_limit': 25,
            'extras': {},
        }
        try:
            with app.test_request_context():
                query_result = test_cube.query(query_obj)
                excepted = '{"cube": "test_cube2", "ordering": [{"field": "Impressions", "order": "DESC"}], "selectFields": [{"field": "Keyword ID"}, {"field": "Keyword Value"}, {"field": "Keyword Status"}, {"field": "CPC"}, {"field": "Keyword Match Type"}, {"field": "CTR"}, {"field": "Post Click Conversions"}, {"field": "Keyword Match Type Full"}, {"field": "Impressions"}, {"field": "Clicks"}, {"field": "Spend"}], "si": 0, "filterExpressions": [{"operator": "not in", "field": "Section Name", "values": ["0"]}, {"operator": "not in", "field": "Site Name", "values": ["0"]}, {"operator": "between", "field": "Day", "from": "2018-06-23", "to": "2018-06-30"}], "mr": 200}'
                self.assertEqual(query_result.query, excepted)
        except Exception as e:
            self.fail("raised ExceptionType unexpectedly! ERROR: {}".format(e))
 
        # with granularity Day
        # Day in group by
        # should contain one Day field
        query_obj = {
            'granularity': '1 day',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'is_timeseries': "True",
            'groupby': ['Day', 'Keyword ID', 'Keyword Value', 'Keyword Status', 'CPC', 'Keyword Match Type', 'CTR', 'Post Click Conversions', 'Keyword Match Type Full'],
            'metrics': ['Impressions', 'Clicks', 'Spend'],
            'row_limit': 200,
            'filter': [{'col': 'Section Name', 'val': ['0'], 'op': 'not in'}, {'col': 'Site Name', 'val': ['0'], 'op': 'not in'}],
            'timeseries_limit': 25,
            'extras': {},
        }
        try:
            with app.test_request_context():
                query_result = test_cube.query(query_obj)
                excepted = '{"cube": "test_cube2", "ordering": [{"field": "Impressions", "order": "DESC"}], "selectFields": [{"field": "Day"}, {"field": "Keyword ID"}, {"field": "Keyword Value"}, {"field": "Keyword Status"}, {"field": "CPC"}, {"field": "Keyword Match Type"}, {"field": "CTR"}, {"field": "Post Click Conversions"}, {"field": "Keyword Match Type Full"}, {"field": "Impressions"}, {"field": "Clicks"}, {"field": "Spend"}], "si": 0, "filterExpressions": [{"operator": "not in", "field": "Section Name", "values": ["0"]}, {"operator": "not in", "field": "Site Name", "values": ["0"]}, {"operator": "between", "field": "Day", "from": "2018-06-23", "to": "2018-06-30"}], "mr": 200}'
                self.assertEqual(query_result.query, excepted)
        except Exception as e:
            self.fail("raised ExceptionType unexpectedly! ERROR: {}".format(e))
 
        # with granularity all
        # Day in group by
        # should contain Day field
        query_obj = {
            'granularity': 'all',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'is_timeseries': "True",
            'groupby': ['Day', 'Keyword ID', 'Keyword Value', 'Keyword Status', 'CPC', 'Keyword Match Type', 'CTR', 'Post Click Conversions', 'Keyword Match Type Full'],
            'metrics': ['Impressions', 'Clicks', 'Spend'],
            'row_limit': 200,
            'filter': [{'col': 'Section Name', 'val': ['0'], 'op': 'not in'}, {'col': 'Site Name', 'val': ['0'], 'op': 'not in'}],
            'timeseries_limit': 25,
            'extras': {},
        }
        try:
            with app.test_request_context():
                query_result = test_cube.query(query_obj)
                excepted = '{"cube": "test_cube2", "ordering": [{"field": "Impressions", "order": "DESC"}], "selectFields": [{"field": "Day"}, {"field": "Keyword ID"}, {"field": "Keyword Value"}, {"field": "Keyword Status"}, {"field": "CPC"}, {"field": "Keyword Match Type"}, {"field": "CTR"}, {"field": "Post Click Conversions"}, {"field": "Keyword Match Type Full"}, {"field": "Impressions"}, {"field": "Clicks"}, {"field": "Spend"}], "si": 0, "filterExpressions": [{"operator": "not in", "field": "Section Name", "values": ["0"]}, {"operator": "not in", "field": "Site Name", "values": ["0"]}, {"operator": "between", "field": "Day", "from": "2018-06-23", "to": "2018-06-30"}], "mr": 200}'
                self.assertEqual(query_result.query, excepted)
        except Exception as e:
            self.fail("raised ExceptionType unexpectedly! ERROR: {}".format(e))
 
        # with granularity Day
        # no Day in group by
        # should contain Day field
        query_obj = {
            'granularity': '1 day',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'is_timeseries': "True",
            'groupby': ['Keyword ID', 'Keyword Value', 'Keyword Status', 'CPC', 'Keyword Match Type', 'CTR', 'Post Click Conversions', 'Keyword Match Type Full'],
            'metrics': ['Impressions', 'Clicks', 'Spend'],
            'row_limit': 200,
            'filter': [{'col': 'Section Name', 'val': ['0'], 'op': 'not in'}, {'col': 'Site Name', 'val': ['0'], 'op': 'not in'}],
            'timeseries_limit': 25,
            'extras': {},
        }
        try:
            with app.test_request_context():
                query_result = test_cube.query(query_obj)
                excepted = '{"cube": "test_cube2", "ordering": [{"field": "Impressions", "order": "DESC"}], "selectFields": [{"field": "Day"}, {"field": "Keyword ID"}, {"field": "Keyword Value"}, {"field": "Keyword Status"}, {"field": "CPC"}, {"field": "Keyword Match Type"}, {"field": "CTR"}, {"field": "Post Click Conversions"}, {"field": "Keyword Match Type Full"}, {"field": "Impressions"}, {"field": "Clicks"}, {"field": "Spend"}], "si": 0, "filterExpressions": [{"operator": "not in", "field": "Section Name", "values": ["0"]}, {"operator": "not in", "field": "Site Name", "values": ["0"]}, {"operator": "between", "field": "Day", "from": "2018-06-23", "to": "2018-06-30"}], "mr": 200}'
                self.assertEqual(query_result.query, excepted)
        except Exception as e:
            self.fail("raised ExceptionType unexpectedly! ERROR: {}".format(e))
 
        # with granularity Day
        # Hour in group by
        # should contain Day and Hour field
        query_obj = {
            'granularity': '1 day',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'is_timeseries': "True",
            'groupby': ['Hour', 'Keyword ID', 'Keyword Value', 'Keyword Status', 'CPC', 'Keyword Match Type', 'CTR', 'Post Click Conversions', 'Keyword Match Type Full'],
            'metrics': ['Impressions', 'Clicks', 'Spend'],
            'row_limit': 200,
            'filter': [{'col': 'Section Name', 'val': ['0'], 'op': 'not in'}, {'col': 'Site Name', 'val': ['0'], 'op': 'not in'}],
            'timeseries_limit': 25,
            'extras': {},
        }
        try:
            with app.test_request_context():
                query_result = test_cube.query(query_obj)
                excepted = '{"cube": "test_cube2", "ordering": [{"field": "Impressions", "order": "DESC"}], "selectFields": [{"field": "Day"}, {"field": "Hour"}, {"field": "Keyword ID"}, {"field": "Keyword Value"}, {"field": "Keyword Status"}, {"field": "CPC"}, {"field": "Keyword Match Type"}, {"field": "CTR"}, {"field": "Post Click Conversions"}, {"field": "Keyword Match Type Full"}, {"field": "Impressions"}, {"field": "Clicks"}, {"field": "Spend"}], "si": 0, "filterExpressions": [{"operator": "not in", "field": "Section Name", "values": ["0"]}, {"operator": "not in", "field": "Site Name", "values": ["0"]}, {"operator": "between", "field": "Day", "from": "2018-06-23", "to": "2018-06-30"}], "mr": 200}'
                self.assertEqual(query_result.query, excepted)
        except Exception as e:
            self.fail("raised ExceptionType unexpectedly! ERROR: {}".format(e))
     
    def test_lookback_cube_with_default_setting_error(self):
        print("==========Starting test_lookback_error==========")
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_cube2').first())
 
        since = "50 days ago"
        from_dttm = utils.parse_human_datetime(since)
        until = "now"
        to_dttm = utils.parse_human_datetime(until)
 
        query_obj = {
            'granularity': 'year',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'is_timeseries': "True",
            'groupby': ['Keyword ID', 'Day', 'Keyword Value', 'Keyword Status', 'CPC', 'Keyword Match Type', 'CTR', 'Post Click Conversions', 'Keyword Match Type Full'],
            'metrics': ['Impressions', 'Clicks', 'Spend'],
            'row_limit': 200,
            'filter': [{'col': 'Section Name', 'val': ['0'], 'op': 'not in'}, {'col': 'Site Name', 'val': ['0'], 'op': 'not in'}],
            'timeseries_limit': 25,
            'extras': {},
        }
 
        with app.test_request_context():
            self.assertRaises(Exception, test_cube.query, query_obj)
 
    def test_lookback_cube_with_default_setting_error(self):
        print("==========Starting test_lookback_error==========")
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_cube2').first())
 
        since = "32 days ago"
        from_dttm = utils.parse_human_datetime(since)
        until = "now"
        to_dttm = utils.parse_human_datetime(until)
 
        query_obj = {
            'granularity': 'year',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'is_timeseries': "True",
            'groupby': ['Keyword ID', 'Day', 'Keyword Value', 'Keyword Status', 'CPC', 'Keyword Match Type', 'CTR', 'Post Click Conversions', 'Keyword Match Type Full'],
            'metrics': ['Impressions', 'Clicks', 'Spend'],
            'row_limit': 200,
            'filter': [{'col': 'Section Name', 'val': ['0'], 'op': 'not in'}, {'col': 'Site Name', 'val': ['0'], 'op': 'not in'}],
            'timeseries_limit': 25,
            'extras': {},
        }
 
        with app.test_request_context():
            self.assertRaises(Exception, test_cube.query, query_obj)
     
    def test_lookback_cube_with_default_setting_sunccess(self):
        print("==========Starting test_lookback_window_success==========")
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_cube2').first())
 
        since = "7 days ago"
        from_dttm = utils.parse_human_datetime(since)
        until = "now"
        to_dttm = utils.parse_human_datetime(until)
        query_obj = {
            'granularity': 'year',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'is_timeseries': "True",
            'groupby': ['Keyword ID', 'Day', 'Keyword Value', 'Keyword Status', 'CPC', 'Keyword Match Type', 'CTR', 'Post Click Conversions', 'Keyword Match Type Full'],
            'metrics': ['Impressions', 'Clicks', 'Spend'],
            'row_limit': 200,
            'filter': [{'col': 'Section Name', 'val': ['0'], 'op': 'not in'}, {'col': 'Site Name', 'val': ['0'], 'op': 'not in'}],
            'timeseries_limit': 25,
            'extras': {},
        }
 
        try:
            with app.test_request_context():
                query_result = test_cube.query(query_obj)
        except Exception as e:
            self.fail("raised ExceptionType unexpectedly! ERROR: {}".format(e))
 
    def test_window_error(self):
        print("==========Starting test_window_error==========")
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_supply_stats_cube').first())
 
        since = "50 days ago"
        from_dttm = utils.parse_human_datetime(since)
        until = "now"
        to_dttm = utils.parse_human_datetime(until)
 
        query_obj = {
            'granularity': 'year',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'is_timeseries': "True",
            'groupby': ['Keyword ID', 'Day', 'Keyword Value', 'Keyword Status', 'CPC', 'Keyword Match Type', 'CTR', 'Post Click Conversions', 'Keyword Match Type Full'],
            'metrics': ['Impressions', 'Clicks', 'Spend'],
            'row_limit': 200,
            'filter': [{'col': 'Section Name', 'val': ['0'], 'op': 'not in'}, {'col': 'Site Name', 'val': ['0'], 'op': 'not in'}],
            'timeseries_limit': 25,
            'extras': {},
        }
 
        with app.test_request_context():
            self.assertRaises(Exception, test_cube.query, query_obj)
     
    def test_lookback_window_success(self):
        print("==========Starting test_lookback_window_success==========")
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_supply_stats_cube').first())
 
        since = "7 days ago"
        from_dttm = utils.parse_human_datetime(since)
        until = "now"
        to_dttm = utils.parse_human_datetime(until)
        #qry_start_dttm = utils.parse_human_datetime(until)
 
        query_obj = {
            'granularity': 'year',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'is_timeseries': "True",
            'groupby': ['Keyword ID', 'Day', 'Keyword Value', 'Keyword Status', 'CPC', 'Keyword Match Type', 'CTR', 'Post Click Conversions', 'Keyword Match Type Full'],
            'metrics': ['Impressions', 'Clicks', 'Spend'],
            'row_limit': 200,
            'filter': [{'col': 'Section Name', 'val': ['0'], 'op': 'not in'}, {'col': 'Site Name', 'val': ['0'], 'op': 'not in'}],
            'timeseries_limit': 25,
            'extras': {},
        }
 
        try:
            with app.test_request_context():
                query_result = test_cube.query(query_obj)
        except Exception as e:
            self.fail("raised ExceptionType unexpectedly! ERROR: {}".format(e))
 
    def test_registry_deletion(self):
        session = get_session()
        test_registry = (
            session
                .query(MahaRegistry)
                .filter_by(registry_name="test_registry")
                .first()
        )
 
        test_supply_stats_cube = (
            session
                .query(Cube)
                .filter_by(cube_name="test_supply_stats_cube")
                .first()
        )
 
        test_country_iso_code_col = (
            session
                .query(MahaColumn)
                .filter_by(column_name='Test Country ISO Code')
                .first()
        )
 
        test_day_col = (
            session
                .query(MahaColumn)
                .filter_by(column_name='Test Day')
                .first()
        )
 
        test_fact_col = (
            session
                .query(CubeMetric)
                .filter_by(metric_name='Test Fact')
                .first()
        )
 
        assert test_registry is not None
        assert test_supply_stats_cube is not None
        assert test_country_iso_code_col is not None
        assert test_day_col is not None
        assert test_fact_col is not None
 
        session.delete(test_registry)
        session.commit()
 
        test_registry = (
            session
                .query(MahaRegistry)
                .filter_by(registry_name="test_registry")
                .first()
        )
 
        test_supply_stats_cube = (
            session
                .query(Cube)
                .filter_by(cube_name="test_supply_stats_cube")
                .first()
        )
 
        test_country_iso_code_col = (
            session
                .query(MahaColumn)
                .filter_by(column_name='Test Country ISO Code')
                .first()
        )
 
        test_day_col = (
            session
                .query(MahaColumn)
                .filter_by(column_name='Test Day')
                .first()
        )
 
        test_metric = (
            session
                .query(CubeMetric)
                .filter_by(metric_name='Test Fact')
                .first()
        )
 
        test_slice = (
            session
                .query(Slice)
                .filter_by(slice_name="Test Slice")
                .first()
        )
 
        assert test_registry is None
        assert test_supply_stats_cube is None
        assert test_day_col is None
        assert test_country_iso_code_col is None
        assert test_metric is None
     
    def test_hourly_filter(self):
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_supply_stats_cube').first())
        qry_start_dttm = datetime.now()
        from_dttm = datetime.now() - timedelta(hours=40)
        to_dttm = datetime.now()
        groupby = []
        metrics = ['Impressions']
        query_obj = {
            'granularity': '1 hour',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'groupby': groupby,
            'metrics': metrics,
        }
        query_str = test_cube.get_query_str("", qry_start_dttm, **query_obj)
        query_json = json.loads(query_str)
        expected_hour_filter = {"operator": "between", "field": "Hour", "from": str(from_dttm.hour), "to": str(to_dttm.hour)}
        excepted_day_filter = {"operator": "between", "field": "Day", "from": str(from_dttm.date()), "to": str(to_dttm.date())}
        assert(expected_hour_filter in query_json['filterExpressions'])
        assert(excepted_day_filter in query_json['filterExpressions'])
 
    def test_nonhourly_filter_for_x_hours_data_retieval(self):
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_supply_stats_cube').first())
        qry_start_dttm = datetime.now()
        from_dttm = datetime.now() - timedelta(hours=3)
        to_dttm = datetime.now()
        groupby = []
        metrics = ['Impressions']
        query_obj = {
            'granularity': '1 day',
            'from_dttm': from_dttm,
            'to_dttm': to_dttm,
            'groupby': groupby,
            'metrics': metrics,
            'extras': {'since':'3 hours ago'}
        }
        query_str = test_cube.get_query_str("", qry_start_dttm, **query_obj)
        query_json = json.loads(query_str)
        expected_hour_filter = {"operator": "between", "field": "Hour", "from": str(from_dttm.hour), "to": str(to_dttm.hour)}
        excepted_day_filter = {"operator": "between", "field": "Day", "from": str(from_dttm.date()), "to": str(to_dttm.date())}
        assert(expected_hour_filter in query_json['filterExpressions'])
        assert(excepted_day_filter in query_json['filterExpressions'])
 
    def test_hourly_grain_returned_for_hourly_graph(self):
        session = get_session()
        test_cube = (session.query(Cube).filter_by(cube_name='test_supply_stats_cube').first())
 
        mock_from_time = "2017-08-06 15"
        from_dttm = utils.parse_human_datetime(mock_from_time)
        mock_to_time = "2017-08-07 15"
        to_dttm = utils.parse_human_datetime(mock_to_time)
 
        query_obj = {
                'granularity': '1 hour', 
                'from_dttm': from_dttm, 
                'to_dttm': to_dttm, 
                'groupby': [], 
                'metrics': ['Impressions', 'Clicks', 'Spend'],
                'row_limit': 200, 
        }
 
        try:
            with app.test_request_context():
                query_result = test_cube.query(query_obj)
                expected = '{"cube": "test_supply_stats_cube", "ordering": [{"field": "Impressions", "order": "DESC"}], "selectFields": [{"field": "Hour"}, {"field": "Day"}, {"field": "Impressions"}, {"field": "Clicks"}, {"field": "Spend"}], "si": 0, "filterExpressions": [{"operator": "between", "field": "Day", "from": "2017-08-06", "to": "2017-08-07"}, {"operator": "between", "field": "Hour", "from": "15", "to": "15"}], "mr": 200}'
                self.assertEqual(query_result.query, expected)
        except Exception as e:
            self.fail("raised ExceptionType unexpectedly! ERROR: {}".format(e))
    
    def clearDBData(self):
        session = get_session()
        test_registry = (
            session
                .query(MahaRegistry)
                .filter_by(registry_name="test_registry")
                .all()
        )
        test_supply_stats_cubes = (
            session
                .query(Cube)
                .filter_by(cube_name="test_supply_stats_cube")
                .all()
        )
        test_country_iso_code_cols = (
            session
                .query(MahaColumn)
                .filter_by(column_name='Test Country ISO Code')
                .all()
        )
        test_day_cols = (
            session
                .query(MahaColumn)
                .filter_by(column_name='Test Day')
                .all()
        )
        test_fact_cols = (
            session
                .query(CubeMetric)
                .filter_by(metric_name='Test Fact')
                .all()
        )

        if test_fact_cols:
            print("Deleting test_fact_cols")
            for col in test_fact_cols:
                session.delete(col)

        if test_country_iso_code_cols:
            print("Deleting test_country_iso_code_cols ...")
            for col in test_country_iso_code_cols:
                session.delete(col)

        if test_day_cols:
            print("Deleting test_day_cols ...")
            for col in test_day_cols:
                session.delete(col)

        if test_registry:
            print("Deleting test_registry ...")
            for col in test_registry:
                session.delete(col)
        if test_supply_stats_cubes:     
            print("Deleting test_supply_stats_cubes ...")     
            for cube in test_supply_stats_cubes:       
                session.delete(cube)
        session.commit()

    def tearDown(self):
        self.clearDBData()
        httpretty.disable()

    
if __name__ == '__main__':
    unittest.main()
