from superset.views.base import SupersetModelView, DeleteMixin
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder import BaseView, expose
from superset import appbuilder, app, db, security_manager, get_session, models
from flask_babel import gettext as _
from flask_appbuilder.security.decorators import has_access
from flask import redirect, flash, Response
from datetime import datetime
import logging
from superset.connectors.maha.models import MahaRegistry, Cube
from superset.models.core import Slice
import calendar, time
import requests
import sqlalchemy as sqla
from flask_appbuilder.actions import action
from superset.utils import flasher
import os.path

sm = appbuilder.sm

class MahaRegistryModelView(SupersetModelView):  # noqa
    datamodel = SQLAInterface(MahaRegistry)
    add_columns = [
        'registry_name',
        'registry_host', 'registry_port', 'registry_endpoint', 'sync_request_endpoint', 'domain_endpoint',
        'white_list', 'black_list'
    ]
    edit_columns = add_columns
    list_columns = ['registry_name', 'metadata_last_refreshed', 'changed_by_']
    order_columns = ['registry_name', 'metadata_last_refreshed']
    
    @action(
        "muldelete", "Delete", "Delete all Really?", "fa-trash", single=False)
    def muldelete(self, registries):
        if not registries:
            abort(404)
        for registry in registries:
            try:
                self.pre_delete(registry)
            except Exception as e:
                flash(str(e), "danger")
            else:
                if self.datamodel.delete(item):
                    self.post_delete(item)
                flash(*self.datamodel.message)
        
        self.update_redirect()
        return redirect(self.get_redirect())

    def pre_delete(self, registry):
        for cube in registry.cubes:
            slices = db.session.query(Slice).filter_by(datasource_id=cube.id).all()
            if slices:
                raise Exception("There are {} slices associated with cube {}, please delete the slices before deleting the registry."
                    .format(len(slices), cube.cube_name))


    def pre_add(self, registry):
        security_manager.merge_perm('database_access', registry.perm)
        if registry.white_list and registry.black_list:
            raise Exception("Fill in either white list or black list, not both")

    def pre_update(self, registry):
        self.pre_add(registry)    


appbuilder.add_view(
    MahaRegistryModelView,
    "Maha Registry",
    icon="fa-cubes",
    category=_("Sources"),
    category_icon='fa-database',)

class MahaCubeModelView(SupersetModelView):
    datamodel = SQLAInterface(Cube)
    add_columns = [
        'registry', 'created_on', 'cube_name', 'owner', 'changed_on', 'schema', 'description'
    ]
    list_columns = [
        'datasource_link', 'registry_name', 'changed_by_', 'created_on', 'changed_on']
    order_columns = [
        'datasource_link', 'registry_name', 'created_on', 'changed_on']
    edit_columns = [
        'cube_version'
    ]
    page_size = 500
    base_order = ('cube_name', 'asc')

    @action(
        "muldelete", "Delete", "Delete all Really?", "fa-trash", single=False)
    def muldelete(self, cubes):
        if not cubes:
            abort(404)
        for cube in cubes:
            try:
                self.pre_delete(cube)
            except Exception as e:
                flash(str(e), "danger")
            else:
                if self.datamodel.delete(item):
                    self.post_delete(item)
                flash(*self.datamodel.message)
        
        self.update_redirect()
        return redirect(self.get_redirect())

    def pre_add(self, datasource):
        number_of_existing_datasources = db.session.query(
            sqla.func.count('*')).filter(
            maha_models.Cube.cube_name ==
            datasource.cube_name,
            reporting_models.Cube.registry_id == datasource.registry.id
        ).scalar()

        # table object is already added to the session
        if number_of_existing_datasources > 1:
            raise Exception(get_datasource_exist_error_mgs(
                datasource.full_name))

    def post_add(self, datasource):
        datasource.generate_metrics()
        security_manager.merge_perm('datasource_access', datasource.perm)

    def post_update(self, datasource):
        self.post_add(datasource)

    def pre_delete(self, datasource):
        slices = db.session.query(Slice).filter_by(datasource_id=datasource.id).all()
        if slices:
            raise Exception("There are {} slices associated with datasource {}, please delete the slices before deleting the cube."
                .format(len(slices), datasource.cube_name))

appbuilder.add_view(
    MahaCubeModelView,
    "Maha Cubes",
    label=_("Maha Cubes"),
    category=_("Sources"),
    icon="fa-cube",
    category_icon='fa-database',)


class Maha(BaseView):

    @expose("/mahaping/")
    def ping(self):
        return "OK"

    @has_access
    @expose("/refresh_maha_datasources/")
    def refresh_reporting_datasources(self):
        """endpoint that refreshes druid datasources metadata"""
        session = db.session()
        for registry in session.query(MahaRegistry).all():
            try:
                registry.refresh_datasources()
            except Exception as e:
                flash(
                    "Error while processing registry '{}'\n{}".format(
                        registry.registry_name, str(e)),
                    "danger")
                logging.exception(e)
                return redirect('/mahacubemodelview/list/')
            registry.metadata_last_refreshed = datetime.now()
            flash(
                "Refreshed metadata from registry "
                "[" + registry.registry_name + "]",
                'info')
        session.commit()
        return redirect("/mahacubemodelview/list/")

appbuilder.add_view_no_menu(Maha)

appbuilder.add_link(
    _("Refresh Maha Registry Metadata"),
    href='/maha/refresh_maha_datasources/',
    category=_('Sources'),
    category_icon='fa-database',
    icon="fa-cog")

appbuilder.add_separator("Sources")

