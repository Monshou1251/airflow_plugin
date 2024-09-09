import json

import flask
import pandas as pd
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request, jsonify, url_for, redirect, flash
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from wtforms import Form, SelectField, RadioField, StringField, BooleanField, DateTimeLocalField, TimeField, DateField
from airflow.www.app import csrf
from wtforms.validators import InputRequired
from croniter import croniter, CroniterBadCronError, CroniterBadDateError

from airflow import settings
from airflow.models import Connection
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook as PH
from airflow.providers.exasol.hooks.exasol import ExasolHook as EH


#  Инициализация фронт-части плагина
bp = Blueprint(
    "project_data_saving",
    __name__,
    template_folder="templates",
)


def get_connection_postgres():
    """Получение хука Postgres"""
    pg_hook = PH.get_hook("airflow_postgres")
    return pg_hook


def get_all_database_mssql():
    """Получение connections из базы данных mssql"""
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_af_net')
    sql = "SELECT name, database_id FROM sys.databases;"
    databases = [i[0] for i in mssql_hook.get_records(sql)]
    return databases


def get_all_schema_exasol():
    """Получение connections из базы данных exasol"""
    exasol_hook = EH(conn_name_attr='exa_af_net')
    sql = "SELECT SCHEMA_NAME FROM EXA_ALL_SCHEMAS;"
    databases = [i[0] for i in exasol_hook.get_records(sql)]
    return databases


def get_all_connections():
    """Получаем все Connections из Apache Airflow"""
    session = settings.Session()
    connections = session.query(Connection).all()
    connections_list = [i.conn_id for i in connections]
    return connections_list


def validate_cron(form, field) -> bool:
    """Кастомный валидатор Cron выражений"""
    cron = field.data
    try:
        croniter(cron)
        return True
    except (CroniterBadCronError, CroniterBadDateError):
        return False


def replace_response_datetime(raw_datetime: str) -> str:
    if raw_datetime is None:
        return 'NULL'
    else:
        return f"'{raw_datetime}'"


class ProjectForm(Form):
    """Form administration of ct project"""

    ct_project_id = StringField(
        'CT Project ID',
        validators=[InputRequired()],
        id="conn_id",
        render_kw={"placeholder": "Type project id",
                   "class": "form-control",
                   "type": "text"
                   }
    )

    source_connection_id = SelectField(
        'Source Connection ID',
        validators=[InputRequired()],
        choices=get_all_connections(),
        id="conn_type",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   },
    )

    one_c_database = SelectField(
        '1C Database',
        validators=[InputRequired()],
        choices=get_all_database_mssql(),
        id="conn_type1",
        name="conn_type1",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    biview_database = SelectField(
        'BIView Database',
        validators=[InputRequired()],
        choices=get_all_database_mssql(),
        id="conn_type2",
        name="conn_type2",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    biview_project_type = RadioField(
        'BIView Project Type',
        validators=[InputRequired()],
        choices=[('1', 'Type 1'), ('2', 'Type 2')],
        default='1',
        name="project_type",
        id="project_type",
        render_kw={"class": "form-check-input",
                   "type": "radio"
                   }
    )

    ct_database = SelectField(
        'CT Database',
        validators=[InputRequired()],
        choices=get_all_database_mssql(),
        id="conn_type3",
        name="conn_type3",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    transfer_source_data = BooleanField(
        'Transfer Source Data'
    )

    target_connection_id = SelectField(
        'Target Connection ID',
        choices=get_all_connections(),
        id="conn_type",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   },
    )

    target_schema = SelectField(
        'Target Database',
        choices=get_all_schema_exasol(),
        id="conn_type4",
        name="conn_type4",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    target_type = SelectField(
        'Target Type',
        default=' ',
        choices=['ODS', 'HODS'],
        id="conn_type5",
        name="conn_type5",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   },
    )

    update_dags_start_date = DateField('Start Date',
                                       render_kw={"class": "form-control-short"}
                                       )
    update_dags_start_time = TimeField('Start time (UTC)')

    update_dags_schedule = StringField('Schedule',
                                       validators=[validate_cron],
                                       id="schedule",
                                       render_kw={"class": "form-control-short",
                                                  "placeholder": "* * * * *"
                                                  }
                                       )

    transfer_dags_start_date = DateField('Start Date',
                                         render_kw={"class": "form-control-short"}
                                         )

    transfer_dags_start_time = TimeField('Start time (UTC)')

    transfer_dags_schedule = StringField('Schedule',
                                         validators=[validate_cron],
                                         id="schedule",
                                         render_kw={"class": "form-control-short",
                                                    "placeholder": "* * * * *"
                                                    }
                                         )


class ProjectsView(AppBuilderBaseView):
    """View of projects"""
    default_view = "project_list"

    @expose('/', methods=['GET'])
    def project_list(self):
        """View list of projects"""
        sql_query = """
                        SELECT
                            ct_project_id,
                            source_connection_id,
                            one_c_database,
                            biview_database,
                            biview_project_type,
                            ct_database,
                            transfer_source_data,
                            target_connection_id,
                            target_schema,
                            target_type 
                        FROM airflow.atk_ct.ct_projects
                    """
        with get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)

                try:
                    columns = [field.label.text for field in ProjectForm()]
                    rows = cursor.fetchall()
                    raw_projects = [dict(zip(columns, row)) for row in rows]

                    projects = []
                    for dictionary in raw_projects:
                        if dictionary['Transfer Source Data'] is False:
                            dictionary['Transfer Source Data'] = 'No'
                            projects.append(dictionary)
                        else:
                            dictionary['Transfer Source Data'] = 'Yes'
                            projects.append(dictionary)
                except Exception as e:
                    flash(str(e), category="error")

        return self.render_template("project_change_tracking.html",
                                    projects=raw_projects,
                                    count_projects=len(raw_projects))

    @expose("/add", methods=['GET', 'POST'])
    @csrf.exempt
    def project_add_data(self):
        """Add CT Project"""

        form = ProjectForm()

        if request.method == 'POST':

            form = ProjectForm(request.form)

            sql_insert_query = f"""
                                INSERT INTO airflow.atk_ct.ct_projects (
                                    ct_project_id,
                                    source_connection_id,
                                    one_c_database,
                                    biview_database,
                                    biview_project_type,
                                    ct_database,
                                    transfer_source_data,
                                    target_connection_id,
                                    target_schema,
                                    target_type,
                                    update_dags_start_date,
                                    update_dags_start_time,
                                    update_dags_schedule,
                                    transfer_dags_start_date,
                                    transfer_dags_start_time,
                                    transfer_dags_schedule
                                    )
                                VALUES (
                                    '{form.ct_project_id.data}',
                                    '{form.source_connection_id.data}',
                                    '{form.one_c_database.data}',
                                    '{form.biview_database.data}',
                                    {form.biview_project_type.data},
                                    '{form.ct_database.data}',
                                    {form.transfer_source_data.data},
                                    '{form.target_connection_id.data}',
                                    '{form.target_schema.data}',
                                    '{form.target_type.data}',
                                    {replace_response_datetime(form.update_dags_start_date.data)},
                                    {replace_response_datetime(form.update_dags_start_time.data)},
                                    '{form.update_dags_schedule.data}',
                                    {replace_response_datetime(form.transfer_dags_start_date.data)},
                                    {replace_response_datetime(form.transfer_dags_start_time.data)},
                                    '{form.transfer_dags_schedule.data}'
                                    );"""
            print(sql_insert_query)
            try:
                with get_connection_postgres().get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_insert_query)
                    conn.commit()

                flash("Проект успешно сохранен", category="info")
                return self.render_template("add_projects.html", form=form)
            except Exception as e:
                if 'duplicate key' in str(e):
                    flash("Данное имя проекта уже существует! Выберите другое.", category='warning')
                else:
                    flash(str(e), category='warning')
                return self.render_template("add_projects.html", form=form)

        return self.render_template("add_projects.html", form=form)

    @expose("/edit/<string:ct_project_id>", methods=['GET', 'POST'])
    @csrf.exempt
    def edit_project_data(self, ct_project_id):
        """Edit of project data"""

        print("Im in edit_project_data")

        sql_select_query = f"""SELECT * FROM airflow.atk_ct.ct_projects WHERE ct_project_id = '{ct_project_id}';"""

        with get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_select_query)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                projects_data = [dict(zip(columns, row)) for row in rows][0]

        form_existing = ProjectForm(data=projects_data)

        form_update = ProjectForm(request.form)

        if request.method == 'POST':

            sql_update_query = f"""
                                UPDATE airflow.atk_ct.ct_projects
                                SET ct_project_id = '{form_update.ct_project_id.data}',
                                    source_connection_id = '{form_update.source_connection_id.data}',
                                    one_c_database = '{form_update.one_c_database.data}',
                                    biview_database = '{form_update.biview_database.data}',
                                    biview_project_type = {form_update.biview_project_type.data},
                                    transfer_source_data = {form_update.transfer_source_data.data},
                                    target_connection_id = '{form_update.target_connection_id.data}',
                                    target_schema = '{form_update.target_schema.data}',
                                    target_type = '{form_update.target_type.data}',
                                    update_dags_start_date = {replace_response_datetime(
                                                                form_update.update_dags_start_date.data)},
                                    update_dags_start_time = {replace_response_datetime(
                                                                form_update.update_dags_start_time.data)},
                                    update_dags_schedule = '{form_update.update_dags_schedule.data}',
                                    transfer_dags_start_date = {replace_response_datetime(
                                                                form_update.transfer_dags_start_date.data)},
                                    transfer_dags_start_time = {replace_response_datetime(
                                                                form_update.transfer_dags_start_time.data)},
                                    transfer_dags_schedule = '{form_update.transfer_dags_schedule.data}'
                                WHERE ct_project_id = '{form_update.ct_project_id.data}'
                                ;"""
            print(sql_update_query)

            try:
                with get_connection_postgres().get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_update_query)
                    conn.commit()

                flash("Проект успешно изменен", category="info")
                return self.render_template("edit_project.html", form=form_update)

            except Exception as e:
                if 'duplicate key' in str(e):
                    flash("Данное имя проекта уже существует! Выберите другое.", category='warning')
                elif 'None' in str(e):
                    flash("Введите дату и время!", category='warning')
                else:
                    flash(str(e), category='warning')
                return self.render_template("edit_project.html", form=form_update)

        return self.render_template("edit_project.html", form=form_existing)

    @expose('/projects_to_load', methods=['GET'])
    def projects_to_load(self):
        """Render a new HTML page"""
        project_name = request.args.get('project_name')
        connection = request.args.get('connection')
        return self.render_template("projects_to_load.html", project_name=project_name, connection=connection)

    @expose('/delete/<string:ct_project_id>', methods=['GET'])
    @csrf.exempt
    def delete_ct_project(self, ct_project_id):
        """Delete project"""
        sql_delete_query = """DELETE FROM airflow.atk_ct.ct_projects WHERE ct_project_id = %s"""
        try:
            with get_connection_postgres().get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_delete_query, (ct_project_id,))
                conn.commit()
            flash("Проект успешно удален", category="info")
        except Exception as e:
            flash(str(e))
        return flask.redirect(url_for('ProjectsView.project_list'))


v_appbuilder_view = ProjectsView()
v_appbuilder_package = {
    "name": "Projects",
    "category": "Project Change Tracking",
    "view": v_appbuilder_view
}


class AirflowConnectionPlugin(AirflowPlugin):
    name = "project_list"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
