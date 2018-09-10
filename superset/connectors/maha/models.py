import json
import requests
from datetime import timedelta, datetime, date
import pandas as pd
import os
import logging

from flask_appbuilder import Model
from flask_appbuilder.models.decorators import renders
from flask import escape, Markup, request, has_request_context

from sqlalchemy import (
    Column, Integer, String, ForeignKey, Text, Boolean, DateTime, Date,
    Table, create_engine, MetaData, desc, select, and_, func)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import table, literal_column, text, column

from superset import app, db, get_session, utils, models, appbuilder
from superset.utils import flasher
from superset.models.helpers import AuditMixinNullable, QueryResult
from superset.models.core import Slice, MahaApi, Lookback
from superset.connectors.base.models import BaseDatasource, BaseMetric, BaseColumn
import numpy as np
import calendar, time
import sqlalchemy as sqla

config = app.config
sm = appbuilder.sm

def update_cube(mapper, connection, target):  # noqa
    session = get_session()
    s = session.query(Cube).filter(Cube.registry_id==target.id)

    # set cube perm
    for i in range(len(target.cubes)):
        s.filter(Cube.cube_name==target.cubes[i].cube_name).update({
            Cube.perm: target.cubes[i].get_perm()
        })

def set_perm(mapper, connection, target):  # noqa
    target.perm = target.get_perm()


class MahaRegistry(Model, AuditMixinNullable):

    """ORM model for storing Maha Registry"""

    __tablename__ = 'maha_registry'

    id = Column(Integer, primary_key=True)
    registry_name = Column(String(50))
    registry_host = Column(String(256))
    registry_port = Column(Integer)
    registry_endpoint = Column(String(256))
    metadata_last_refreshed = Column(DateTime)
    cache_timeout = Column(Integer)
    sync_request_endpoint = Column(String(256))
    domain_endpoint = Column(String(256))
    white_list = Column(Text)
    black_list = Column(Text)
    cubes = relationship('Cube', cascade='delete')

    @property
    def data(self):
        return {
            'id': self.id,
            'name': self.registry_name,
            'backend': 'maha',
        }

    def get_pydruid_client(self):
        return ""
    
    def get_cubes(self):
        # make get request to http://{hostname}:{port}/{endpoint}/cubes
        rid = MahaApi.get_request_id()
        headers = MahaApi.get_default_headers(rid)

        url = MahaApi.get_domain_url(self)
        cubes_json = json.loads(requests.get(url, headers=headers).text)
        flasher("RID: {}".format(rid))
        all_caravel_cubes = cubes_json['schemas']['caravel']

        exposing_cubes = []
        # go through white/black lists
        if self.white_list:
            list_split = self.white_list.split(",")
            for cube in list_split:
                cube_name = cube.strip()
                if cube_name in all_caravel_cubes:
                    exposing_cubes.append(cube_name)
                else:
                    flasher("Cube {} does not exist in caravel schema".format(cube_name), "warning")
        elif self.black_list:
            list_split = self.black_list.split(",")
            blacklisted_cube_names = []
            for cube in list_split:
                cube_name = cube.strip()
                blacklisted_cube_names.append(cube_name)
            for cube in all_caravel_cubes:
                if cube not in blacklisted_cube_names:
                    exposing_cubes.append(cube)

        if exposing_cubes:
            return exposing_cubes
        return all_caravel_cubes
    
    def is_alive(self):
        """Check if the registry is available"""
        rid = MahaApi.get_request_id()
        headers = MahaApi.get_default_headers(rid)

        url = MahaApi.get_domain_url(self)
        try:
            response = requests.get(url, headers=headers)
            if response.status_code != requests.codes.ok:
                return "Exception when request registry {}: {}, {}".format(self.registry_name, response.status_code, response.text)
            if not response.text:
                return "No data from registry: {}".format(self.registry_name)
        except Exception as e:
            return "Exception when requesting registry {}: {}".format(self.registry_name, e)

        try:
            cubes = self.get_cubes()
        except Exception as e:
            return "Exception when getting cubes from registry {}: {}".format(self.registry_name, e)

        return "OK"
    
    def refresh_datasources(self):
        session = get_session()
        exposing_cubes = self.get_cubes()
        for cube in self.cubes:
            if cube.cube_name not in exposing_cubes:
                logging.info("Deleting cube {} in cluster {}...".format(cube.cube_name, self.registry_name))
                flasher("Deleting cube {} in cluster {}...".format(cube.cube_name, self.registry_name), "info")
                session.delete(cube)
        for cube in exposing_cubes:
            Cube.sync_to_db(cube, self)
    
    @property
    def perm(self):
        return "[{obj.registry_name}].(id:{obj.id})".format(obj=self)
    
sqla.event.listen(MahaRegistry, 'after_insert', update_cube)
sqla.event.listen(MahaRegistry, 'after_update', update_cube)

class CubeMetric(Model, BaseMetric):

    """ORM object referencing Cube metrics for a cube"""

    __tablename__ = 'cube_metrics'
    id = Column(Integer, primary_key=True)
    cube_id = Column(Integer, ForeignKey('cubes.id'))
    # Setting enable_typechecks=False disables polymorphic inheritance.
    cube = relationship('Cube', enable_typechecks=False)
    json = Column(Text)

    export_fields = (
            'metric_name', 'verbose_name', 'metric_type', 'id', 'cube_id',
            'json', 'description', 'is_restricted', 'd3format')

    @property
    def expression(self):
        return self.json

    @property
    def json_obj(self):
        try:
            obj = json.loads(self.json)
        except Exception:
            obj = {}
        return obj

    @property
    def perm(self):
        return (
            "{parent_name}.[{obj.metric_name}](id:{obj.id})"
        ).format(obj=self,
                 parent_name=self.cube.full_name
                 ) if self.cube else None
                 

class MahaColumn(Model, BaseColumn):

    """ORM model for storing cube column metadata"""

    __tablename__ = 'maha_columns'
    id = Column(Integer, primary_key=True)
    cube_id = Column(Integer, ForeignKey('cubes.id'))
    # Setting enable_typechecks=False disables polymorphic inheritance.
    datasource = relationship('Cube', enable_typechecks=False)

    export_fields = (
            'id', 'cube_id', 'column_name', 'verbose_name', 'is_active', 
            'type', 'groupby', 'count_distinct', 'sum', 'avg', 'max', 'min', 
            'filterable', 'description'
    )
    
    @property
    def expression(self):
        return self.description

    @property
    def isFactCol(self):
        return self.type in 'Fact'
    
    def generate_metrics(self):
        """Generate metrics based on the column metadata"""

        M = CubeMetric

        metrics = []

        if self.isFactCol:
            name = self.column_name
            metrics.append(CubeMetric(
                metric_name=name,
                verbose_name=self.column_name,
                json=json.dumps({
                    'name': name, 'fieldName': self.column_name})
            ))

        if self.sum:
            mt = self.type.lower() + 'Sum'
            name = self.column_name
            metrics.append(CubeMetric(
                metric_name=name,
                metric_type='sum',
                verbose_name='SUM({})'.format(self.column_name),
                json=json.dumps({
                    'type': mt, 'name': name, 'fieldName': self.column_name})
            ))
        if self.min:
            mt = self.type.lower() + 'Min'
            name = self.column_name
            metrics.append(CubeMetric(
                metric_name=name,
                metric_type='min',
                verbose_name='MIN({})'.format(self.column_name),
                json=json.dumps({
                    'type': mt, 'name': name, 'fieldName': self.column_name})
            ))
        if self.max:
            mt = self.type.lower() + 'Max'
            name = self.column_name
            metrics.append(CubeMetric(
                metric_name=name,
                metric_type='max',
                verbose_name='MAX({})'.format(self.column_name),
                json=json.dumps({
                    'type': mt, 'name': name, 'fieldName': self.column_name})
            ))
        if self.count_distinct:
            mt = 'count_distinct'
            name = self.column_name
            metrics.append(CubeMetric(
                metric_name=name,
                verbose_name='COUNT(DISTINCT {})'.format(self.column_name),
                metric_type='count_distinct',
                json=json.dumps({
                    'type': 'cardinality',
                    'name': name,
                    'fieldNames': [self.column_name]})
            ))
        session = get_session()
        for metric in metrics:
            m = (
                session.query(M)
                .filter(M.metric_name == metric.metric_name)
                .filter(M.cube_id == self.cube_id)
                .filter(MahaRegistry.registry_name == self.datasource.registry_name)
                .first()
            )
            metric.cube_id = self.cube_id
            if not m:
                session.add(metric)
                session.flush()

class Cube(Model, BaseDatasource, AuditMixinNullable):

    """ORM model for storing Maha Registry cubes"""
    type = "maha_api"

    # TODO:
    baselink = "mahacubemodelview"

    __tablename__ = 'cubes'

    type = "maha_api"
    
    registry_class = MahaRegistry
    metric_class = CubeMetric
    column_class = MahaColumn

    id = Column(Integer, primary_key=True)
    cube_name = Column(String(256))
    is_featured = Column(Boolean, default=False)
    params = Column(String(1000))
    # is_hidden = Column(Boolean, default=False)
    filter_select_enabled = Column(Boolean, default=False)
    description = Column(Text)
    default_endpoint = Column(Text)
    user_id = Column(Integer, ForeignKey('ab_user.id'))
    owner = relationship('User', backref='cubes', foreign_keys=[user_id])
    registry_id = Column(
        Integer, ForeignKey('maha_registry.id'))
    registry = relationship(
        'MahaRegistry', foreign_keys=[registry_id])
    schema = Column(Text)
    offset = Column(Integer, default=0)
    cache_timeout = Column(Integer)
    cube_version = Column(Integer, default=0)
    perm = Column(String(1000))
    columns = relationship('MahaColumn', cascade='delete')
    metrics = relationship('CubeMetric', cascade='delete')
    lookbacks = relationship('Lookback', cascade='delete')
    
    def __repr__(self):
        return self.full_name
    
    @property
    def connection(self):
        return str(self.registry_name)

    @property
    def registry_name(self):
        if (self.registry is None):
            return None
        else:
            return self.registry.registry_name

    @property
    def metrics_combo(self):
        return sorted(
            [(m.metric_name, m.verbose_name) for m in self.metrics],
            key=lambda x: x[1])
          
    @property
    def num_cols(self):
        return [c.column_name for c in self.columns if c.isFactCol]

    @property
    def name(self):
        return self.cube_name

    @property
    def full_name(self):
        return (
            "[{obj.registry_name}]."
            "[{obj.cube_name}]").format(obj=self)

    def get_perm(self):
        return (
            "[{obj.registry_name}].[{obj.cube_name}]"
            "(id:{obj.id})").format(obj=self)

    @property
    def link(self):
        return (
            '<a href="{self.url}">'
            '{self.cube_name}</a>').format(**locals())
            
    @property
    def schema_perm(self):
        """Returns schema permission if present, register one otherwise."""
        return None

    @renders('datasource_name')
    def datasource_link(self):
        url = "/superset/explore/{obj.type}/{obj.id}/".format(obj=self)
        name = escape(self.cube_name)
        return Markup('<a href="{url}">{name}</a>'.format(**locals()))

    @property
    def explore_url(self):
        if self.default_endpoint:
            return self.default_endpoint
        else:
            return "/superset/explore/{obj.type}/{obj.id}/".format(obj=self)

    @property
    def database(self):
        return self.registry
    
    @property
    def time_column_grains(self):
        return {
            "time_columns": [
                'all', '5 seconds', '30 seconds', '1 minute',
                '5 minutes', '1 hour', '6 hour', '1 day', '7 days'
            ],
            "time_grains": ['now']
        }

    def get_metric_obj(self, metric_name):
        return [
            m.json_obj for m in self.metrics
            if m.metric_name == metric_name
        ][0]

    @property
    def datasource_name(self):
        return self.cube_name

    def generate_metrics(self):
        for col in self.columns:
            col.generate_metrics()
    
    @classmethod
    def is_alive(cls, cube_name, registry):
        """Check if the cube is available"""
        session = get_session()
        cube = session.query(cls).filter_by(cube_name=cube_name, registry=registry).first()
        rid = MahaApi.get_request_id()
        headers = MahaApi.get_default_headers(rid)

        if cube is None:
            return "Cube \'{cube}\' not found".format(cube=cube_name)

        url = MahaApi.get_cube_url(registry, cube)
        logging.info("Requesting {} from cube {}.{}".format(url, registry.registry_name, cube.cube_name))
        
        try:
            response = requests.get(url, headers=headers)
            if response.status_code != requests.codes.ok:
                return "Request failed: {}, {}".format(response.status_code, response.text)
            if not response.text:
                return "No data from cube: {}.{}".format(registry.registry_name, cube.cube_name)
        except Exception as e:
            return "Exception when requesting cube {}.{}: {}".format(registry.registry_name, cube.cube_name, e)

        first_metric = session.query(CubeMetric).filter_by(cube_id=cube.id).first().metric_name
        to_dttm = datetime.combine(date.today(), datetime.min.time())
        from_dttm = datetime.combine(date.today() - timedelta(2), datetime.min.time())
        try:
            l = session.query(Lookback).filter_by(cube_id=cube.id, request_type='SyncRequest').first()
            if l and l.grain and l.grain in ('DailyGrain','HourlyGrain','All'):
                if l.grain == "DailyGrain":
                    cube_grain = "1 day"
                elif l.grain == "HourlyGrain":
                    cube_grain = "1 hour"
                else:
                    cube_grain = "all"
                query_response = cube.query_helper(
                    groupby=[],
                    metrics=[first_metric],
                    from_dttm=from_dttm,
                    to_dttm=to_dttm,
                    granularity=cube_grain)
        except utils.NoDataException as e:
            pass
        except Exception as e:
            return "Exception when requesting cube {}.{}: {}".format(registry.registry_name, cube.cube_name, e)

        return "OK"
    
    @classmethod
    def sync_to_db(cls, cube_name, registry):
        """Fetches metadata for that cube and merges the Caravel db"""

        logging.info("Syncing maha cube [{}]".format(cube_name))
        session = get_session()
        cube = session.query(cls).filter_by(cube_name=cube_name, registry=registry).first()
        logging.info("Getting cube {} from registry {}: {}...".format(cube_name, registry.registry_name, cube))
        if not cube:
            logging.info("Creating cube: {} for registry {}".format(cube_name, registry.registry_name))
            cube = cls(cube_name=cube_name)
            session.add(cube)
            flasher("Adding new datasource [{}] for registry {}".format(cube_name, registry.registry_name), "success")
            sm = appbuilder.sm
            perm = cube.get_perm()
            logging.info("test cube perm: {}".format(cube.perm))
            if cube.perm != perm:
                cube.perm = perm
        else:
            flasher("Refreshing datasource [{}] for registry {}".format(cube_name, registry.registry_name), "info")
        cube.registry = registry

        session.commit()
        
        rid = MahaApi.get_request_id()
        headers = MahaApi.get_default_headers(rid)

        url = MahaApi.get_cube_url(registry, cube)
        
        response = requests.get(url, headers=headers)

        if response.status_code != requests.codes.ok:
            logging.exception(response.text)
            raise Exception("RID: {}, Response: {}".format(rid, response.text))  
        

        cube_metadata_json = json.loads(response.text)

        cols = cube_metadata_json['fields']
        mainEntityIds = cube_metadata_json['mainEntityIds']
        schemas = []
        for k in mainEntityIds:
           schemas.append(str(k))
        cube.schema = str(schemas)

        # Delete all existing cube look backs
        all_lookbacks = session.query(Lookback).filter_by(cube_id=cube.id).delete()

        if cube_metadata_json and ('maxDaysWindow' in cube_metadata_json) and ('maxDaysLookBack' in cube_metadata_json):
            lookbacks = cube_metadata_json['maxDaysLookBack']
            max_windows = cube_metadata_json['maxDaysWindow']
            
            # for each lookback update cube lookback
            for lookback in lookbacks:
                request_type = lookback["requestType"]
                grain = lookback["grain"]
                days = lookback["days"]
                l = session.query(Lookback).filter_by(cube_id=cube.id, request_type=request_type, grain=grain).first()
                if not l:
                    l = Lookback(cube_id=cube.id, request_type=request_type, grain=grain)
                    session.add(l)
                l.lookback_days = days


                if grain == 'DailyGrain':
                    l = session.query(Lookback).filter_by(cube_id=cube.id, request_type=request_type, grain='All').first()
                    if not l:
                        l = Lookback(cube_id=cube.id, request_type=request_type, grain='All')
                        session.add(l)
                    l.lookback_days = days

            session.flush()
            # for each max window update cube max windows
            for max_window in max_windows:
                request_type = max_window["requestType"]
                grain = max_window["grain"]
                days = max_window["days"]
                l = session.query(Lookback).filter_by(cube_id=cube.id, request_type=request_type, grain=grain).first()
                if not l:
                    l = Lookback(cube_id=cube.id, request_type=request_type, grain=grain)
                    session.add(l)
                l.max_window = days

                if grain == 'DailyGrain':
                    l = session.query(Lookback).filter_by(cube_id=cube.id, request_type=request_type, grain='All').first()
                    if not l:
                        l = Lookback(cube_id=cube.id, request_type=request_type, grain='All')
                        session.add(l)
                    l.max_window = days


            session.flush()


        existing_cols = (
            session
            .query(MahaColumn)
            .filter_by(cube_id=cube.id)
            .all()
            )

        if not cols:
            existing_cols.delete()
            return

        new_col_names = []
        for col in cols:
            new_col_names.append(col['field'])
            col_obj = (
                session
                .query(MahaColumn)
                .filter_by(cube_id=cube.id, column_name=col['field'])
                .first()
            )

            datatype = col['dataType']['type']
            if not col_obj:
                col_obj = MahaColumn(cube_id=cube.id, column_name=col['field'])
                session.add(col_obj)

            col_obj.filterable = col['filterable']
            col_obj.type = col["type"] # DIM FACT

            if col_obj.type == 'Dimension':
                col_obj.groupby = True

            if 'rollupExpression' in col.keys():
                if col['rollupExpression'] == 'SumRollup':
                    col_obj.sum = True
            session.flush()

            col_obj.datasource = cube
            col_obj.generate_metrics()
            session.flush()

        for col in existing_cols:
            if col.column_name not in new_col_names:
                session.delete(col)
                logging.info("Deleted col: {}".format(col.column_name))
                session.commit()

        existing_metrics = session.query(CubeMetric).filter_by(cube_id=cube.id).all()
        for m in existing_metrics:
            if m.metric_name not in new_col_names:
                session.delete(m)
                logging.info("Deleted metric: {}".format(m.metric_name))
                session.commit()
    
    def get_query_str(
            self, client, qry_start_dttm,
            groupby, metrics,
            granularity,
            from_dttm, to_dttm,
            filter=[],  # noqa
            is_timeseries=True,
            timeseries_limit=None,
            timeseries_limit_metric=None,
            row_limit=1000,
            inner_from_dttm=None, inner_to_dttm=None,
            orderby=None,
            extras=None,  # noqa
            select=None,  # noqa
            columns=None, phase=2):
        # form a request json
        request_data = {}
        request_data['cube'] = self.cube_name
        request_data['filterExpressions'] = []
        # {'price_type' 'in', '1, 3, 6'}
        time_columns = ['__from', '__to']

        for f in filter:
            if 'col' not in f:
                continue
            if f['col'] == '__from' and f['val'] != None and f['val'] != []:
                if inner_from_dttm == None:
                    from_dttm = utils.parse_human_datetime(f['val'])
                else:
                    from_dttm = utils.parse_human_datetime(f['val']) - (inner_from_dttm - from_dttm)
            if f['col'] == '__to' and f['val'] != None and f['val'] != []:
                if inner_to_dttm == None:
                    to_dttm = utils.parse_human_datetime(f['val'])
                else:
                    to_dttm = utils.parse_human_datetime(f['val']) - (inner_to_dttm - to_dttm)
            if f['col'] not in time_columns:
                filter_json = {}
                filter_json["field"] = f['col']
                filter_json["operator"] = f['op']
                filter_vals = f['val']
                filter_json["values"] = self.update_filter_values(filter_vals)
                request_data['filterExpressions'].append(filter_json)

        # get grain of the query
        # append Day or Hour or nothing according to time granularity
        # time granularity reads to 0.0 if it is 'all'
        # human_time_total_secs is 0 if cannot parse the granularity
        query_grain = 'All'
        human_time_total_secs = utils.parse_human_timedelta(granularity).total_seconds()
        ONE_DAY = timedelta(days=1).total_seconds()
        ONE_HOUR = timedelta(hours=1).total_seconds()
        ONE_MINUTE = timedelta(minutes=1).total_seconds()
        if human_time_total_secs == ONE_DAY:
            query_grain = 'DailyGrain'
        if human_time_total_secs == ONE_HOUR:
            query_grain =  "HourlyGrain"
        if human_time_total_secs == 60:
            query_grain = "MinuteGrain"

        session = get_session()
        allowed_max_window = int(config.get("MAX_DAYS_WINDOW_SYNC"))
        allowed_lookback_days = int(config.get("MAX_DAYS_LOOKBACK_SYNC"))

        lookback = session.query(Lookback).filter_by(cube_id=self.id, request_type='SyncRequest', grain=query_grain).first()
        if lookback:
            allowed_max_window = lookback.max_window
            allowed_lookback_days = lookback.lookback_days

        selected_lookback = qry_start_dttm - from_dttm
        if selected_lookback.days > allowed_lookback_days :   #from date is earlier than lookback
            earliest_from_dttm = qry_start_dttm - timedelta(days=allowed_lookback_days - 1)
            logging.info("Earliest date available: {}. You selected {} to {}".format(str(earliest_from_dttm.date()),str(from_dttm.date()),str(to_dttm.date())))
            raise Exception("Earliest date available: {}. You selected {} to {}".format(str(earliest_from_dttm.date()),str(from_dttm.date()),str(to_dttm.date())))

        time_span = to_dttm - from_dttm
        if time_span > timedelta(days=allowed_max_window) : #selected window is greater than than allowed window
            logging.info("Maximum allowed time span: {} days. You selected {} days".format(allowed_max_window,time_span.days))
            raise Exception("Maximum allowed time span: {} days. You selected {} days".format(allowed_max_window,time_span.days))

        # append day fileter
        request_data['filterExpressions'].append({"field": "Day", "operator": "between", "from": str(from_dttm.date()), "to": str(to_dttm.date())})

        request_data['selectFields'] = []

        if query_grain is 'DailyGrain':
            request_data['selectFields'].append({"field": "Day"})
            time_columns.append('Day')
        if query_grain is 'HourlyGrain':
            request_data['selectFields'].append({"field": "Hour"})
            time_columns.append('Hour')
            request_data['filterExpressions'].append({"field": "Hour", "operator": "between", "from": str(from_dttm.hour), "to": str(to_dttm.hour)})
            # for certain cubes, Hour data is only from 00 to 23. So Day data is needed for accurate time. 
            if is_timeseries:
                request_data['selectFields'].append({"field": "Day"})
                time_columns.append('Day')
        if query_grain is 'MinuteGrain':
            request_data['selectFields'].append({"field": "Minute"})
            time_columns.append('Minute')

        #Case when user enters since:x hours ago. If cube has hourly data, show only those hours' data; else show entire days' data.
        hourly_grain_exists = session.query(Lookback).filter_by(cube_id=self.id, request_type='SyncRequest', grain='HourlyGrain').first()
        if query_grain is not 'HourlyGrain' and extras and 'since' in extras and 'hour' in extras['since'] and hourly_grain_exists:
            request_data['filterExpressions'].append({"field": "Hour", "operator": "between", "from": str(from_dttm.hour), "to": str(to_dttm.hour)})


        # add all columns to fields
        for field in groupby:
            if field not in time_columns:
                request_data['selectFields'].append({"field": field})

        for metric in metrics:
            request_data['selectFields'].append({"field": metric})

        request_data['mr'] = min(1000, row_limit)
        request_data['si'] = 0

        # example: "ordering":[{"field":"Ad Group ID","order":"DESC"}]
        request_data['ordering'] = [] # a json list
        if orderby:
            for ob in orderby:
                order = 'DESC'
                if ob[1]:
                    order = 'ASC'
                request_data['ordering'].append({"field": ob[0], "order": order})
        else:
            request_data['ordering'].append({"field": metrics[0], "order": "DESC"})

        request_json = json.dumps(request_data)
        logging.info(request_json)
        return str(request_json)
    
     # for filter value that contains comma, further split the value
    def update_filter_values(self, filter_vals):
        updated_filter_vals = []
        for val in filter_vals:
            if isinstance(val,(int,long,float)):
                stripped_vals = []
                stripped_vals.append(val)
            else:
                splitted_vals = val.split(',')
                stripped_vals = list(map(str.strip, [str(x) for x in splitted_vals]))
            updated_filter_vals += stripped_vals
        return updated_filter_vals
    
    def query_helper(self, groupby, metrics,
            granularity,
            from_dttm, to_dttm,
            filter=[],  # noqa
            is_timeseries=True,
            timeseries_limit=None,
            timeseries_limit_metric=None,
            row_limit=1000,
            inner_from_dttm=None, inner_to_dttm=None,
            orderby=None,
            extras=None,  # noqa
            select=None,  # noqa
            columns=None,
            order_desc=None,
            prequeries=None,
            is_prequery=False,
            form_data=None,phase=2):
        
        # Making request to get json result: 
        # prepare headers
        rid = MahaApi.get_request_id()
        headers = MahaApi.get_sync_request_headers(request,rid)
        # need to figuire out how to decide schema

        params = {"forceRevision": self.cube_version}
        url = MahaApi.get_sync_url(self.registry)
        
        qry_start_dttm = datetime.now()

        query_str = self.get_query_str("", qry_start_dttm, groupby, metrics,
            granularity, from_dttm, to_dttm, filter, is_timeseries, timeseries_limit, timeseries_limit_metric,
            row_limit, inner_from_dttm, inner_to_dttm, orderby, extras, select, columns, phase)
        
        response = requests.post(url, data=query_str, headers=headers, params=params)
        
        response_json = json.loads(response.text)

        if response.status_code == requests.codes.ok:
            fields = []
            for f in response_json['header']['fields']:
                fields.append(f['fieldName'])

            df = pd.DataFrame(response_json['rows'], columns=fields)

            if is_timeseries and ('Day' in fields) and (str(granularity) in ["1 day", "one day"]):
                df['__timestamp'] = df['Day']
            
            if is_timeseries and ('Hour' in fields) and (str(granularity) == "1 hour") and (self.cube_name != "campaign_performance_stats"):
                if (df is not None) and (not df.empty):
                    if df['Hour'].isnull().values.any():
                        raise Exception("There is NULL value in Hour column")
                    if df['Day'].isnull().values.any():
                        raise Exception("There is NULL value in Day column")

                    include_date = (len(unicode(df['Hour'].values[0])) > 2)
                    only_hour  = (len(unicode(df['Hour'].values[0])) in [1, 2])

                    if include_date:
                        df['__timestamp'] = df['Hour']
                        del df['Day']
                    elif only_hour:
                        df['__timestamp'] = yviz.create_time_stamp_with_hour_day_cols(df['Hour'], df['Day'])
                        if 'Day' not in groupby:
                            del df['Day']
            
            if df is None or df.empty:
                logging.warning("No data. RID: {}".format(rid))
                raise utils.NoDataException("No data. RID: {}".format(rid))
            df.replace(np.nan, 'null', inplace=True)

            return QueryResult(
                df=df,
                query=query_str,
                duration=datetime.now() - qry_start_dttm)
        else:
            if 'status' in response_json:
                logging.error(response_json['status'], rid, response_json['detailedMessage'], query_str)
                raise Exception("RID: {}, Response status: {}, Message: {}, Query: {}".format(rid, response_json['status'], response_json['detailedMessage'], query_str))
            elif 'errorMsg' in response_json:
                logging.error('ERROR', rid, response_json['errorMsg'], query_str)
                raise Exception("RID: {}, Response status: ERROR, Message: {}, Query: {}".format(rid, response_json['errorMsg'], query_str))
            logging.error('ERROR', rid, query_str)
            raise Exception("RID: {}, Response status: ERROR, Message: Empty, Query: {}".format(rid, query_str))


    def query(self, query_obj):
        return self.query_helper(**query_obj)


sqla.event.listen(Cube, 'before_insert', set_perm)
sqla.event.listen(Cube, 'before_update', set_perm)

    