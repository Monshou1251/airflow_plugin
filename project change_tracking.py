import json

import flask
import pandas as pd
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request, jsonify, url_for, redirect, flash
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from wtforms import Form, SelectField, RadioField, DateField, StringField, BooleanField
from airflow.www.app import csrf
from wtforms.validators import InputRequired
from croniter import croniter, CroniterBadCronError, CroniterBadDateError

from airflow import settings
from airflow.models import Connection
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook as PH

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


class MyTask:
    def __init__(self, source_connection_id, one_c_database, biview_database, biview_project_type,
                 ct_database, transfer_source_data, target_connection_id, target_database, target_type,
                 ct_project_name):
        self.source_connection_id = source_connection_id
        self.one_c_database = one_c_database
        self.biview_database = biview_database
        self.biview_project_type = biview_project_type
        self.ct_database = ct_database
        self.transfer_source_data = transfer_source_data
        self.target_connection_id = target_connection_id
        self.target_database = target_database
        self.target_type = target_type
        self.ct_project_name = ct_project_name

    def jsonify_data(self) -> str:
        """Transformation data in json format"""
        result = {
            'source_connection_id': self.source_connection_id,
            'one_c_database': self.one_c_database,
            'biview_database': self.biview_database,
            'biview_project_type': self.biview_project_type,
            'ct_database': self.ct_database,
            'transfer_source_data': self.transfer_source_data,
            'target_connection_id': self.target_connection_id,
            'target_database': self.target_database,
            'target_type': self.target_type,
            'ct_project_name': self.ct_project_name,
        }
        return json.dumps(result, indent=4)


class ProjectForm(Form):
    """Форма внесения данных о новых проектах и изменения существующих проектов"""

    ct_project_name = StringField(
        'Project name',
        validators=[InputRequired()],
        id="conn_id",

        render_kw={"placeholder": "Введите имя проекта",
                   "class": "form-control",
                   "type": "text"
                   }
    )

    source_connection_id = SelectField(
        'Source Connection ID',
        choices=get_all_connections(),
        id="conn_type",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   },
    )

    one_c_database = SelectField(
        '1C Database',
        choices=get_all_database_mssql(),
        id="conn_type",
        name="conn_type2",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    biview_database = SelectField(
        'BIView Database',
        choices=get_all_database_mssql(),
        id="conn_type",
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
        choices=get_all_database_mssql(),
        id="conn_type",
        name="conn_type2",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    transfer_source_data = BooleanField(
        'Transfer Source Data to Target',
        false_values=(False, 'No', "Yes")
    )

    target_connection_id = SelectField(
        'Target Connection ID',
        choices=get_all_connections(),
        id="conn_type",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   },
    )

    target_database = SelectField(
        'Target Database',
        choices=get_all_database_mssql(),
        id="conn_type",
        name="conn_type3",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    target_type = SelectField(
        'Target Type',
        choices=['ODS', 'HODS'],
        id="conn_type",
        name="conn_type1",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   },
    )

    # start = DateField('Дата Начала',
    #                   validators=[InputRequired()],
    #                   render_kw={"class": "form-control-short"}
    #                   )
    #
    # schedule = StringField('Расписание',
    #                        validators=[InputRequired(), validate_cron],
    #                        default="* * * * *",
    #                        render_kw={"class": "form-control-short",
    #                                   "data-placeholder": "Select Value"
    #                                   }
    #                        )


class ProjectsView(AppBuilderBaseView):
    """Представление для просмотра, добавления и редактирования проектов"""
    default_view = "project_list"

    @expose('/', methods=['GET'])
    def project_list(self):
        """View list of projects"""
        sql_query = "SELECT * FROM airflow.atk_ct.ct_projects"
        with get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                try:
                    columns = [col[0] for col in cursor.description]
                    rows = cursor.fetchall()
                    projects = [dict(zip(columns, row)) for row in rows]
                    count_projects = len(projects)
                    print(projects)
                except Exception as e:
                    flash(str(e), category="error")
        return self.render_template("project_change_tracking.html", projects=projects, count=count_projects)

    @expose("/add", methods=['GET', 'POST'])
    @csrf.exempt
    def project_add_data(self):
        """Добавление проекта в базу данных"""

        form = ProjectForm(request.form)

        if request.method == 'POST':
            print(form)
            my_task_output = MyTask(
                source_connection_id=form.source_connection_id.data,
                one_c_database=form.one_c_database.data,
                biview_database=form.biview_database.data,
                biview_project_type=form.biview_project_type.data,
                ct_database=form.ct_database.data,
                transfer_source_data=form.transfer_source_data.data,
                target_connection_id=form.target_connection_id.data,
                target_database=form.target_database.data,
                target_type=form.target_type.data,
                ct_project_name=form.ct_project_name.data
            )

            sql_insert_query = f"""
                                INSERT INTO airflow.atk_ct.ct_projects (
                                    ct_project_name,
                                    source_connection_id,
                                    one_c_database,
                                    biview_database,
                                    biview_project_type,
                                    ct_database,
                                    transfer_source_data,
                                    target_connection_id,
                                    target_database,
                                    target_type
                                    )
                                VALUES (
                                    '{my_task_output.ct_project_name}',
                                    '{my_task_output.source_connection_id}',
                                    '{my_task_output.one_c_database}',
                                    '{my_task_output.biview_database}',
                                    {my_task_output.biview_project_type},
                                    '{my_task_output.ct_database}',
                                    {my_task_output.transfer_source_data},
                                    '{my_task_output.target_connection_id}',
                                    '{my_task_output.target_database}',
                                    '{my_task_output.target_type}'
                                    );"""

            try:
                with get_connection_postgres().get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_insert_query)
                    conn.commit()

                flash("Проект успешно сохраненн", category="info")
                return self.render_template("add_projects.html", form=form)

            except Exception as e:
                if 'duplicate key' in str(e):
                    flash("Данное имя проекта уже существует! Выберите другое.", category='warning')
                else:
                    flash(str(e), category='warning')
                return self.render_template("add_projects.html", form=form)

        return self.render_template("add_projects.html", form=form)

    @expose("/edit/<int:ct_project_id>", methods=['GET', 'POST'])
    @csrf.exempt
    def edit_project_data(self, ct_project_id):
        """Редактирование данных проекта"""

        print("Im in edit_project_data")

        sql_select_query = f"""SELECT * FROM airflow.atk_ct.ct_projects WHERE ct_project_id = '{ct_project_id}';"""

        with get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_select_query)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                projects_data = [dict(zip(columns, row)) for row in rows][0]

        # projects_data['start_date'] = projects_data['start_date'].strftime('%d.%m.%Y')
        print(projects_data)

        form_existing = ProjectForm(data=projects_data)

        form_update = ProjectForm(request.form)

        if request.method == 'POST':

            my_task_output = MyTask(
                source_connection_id=form_update.source_connection_id.data,
                one_c_database=form_update.one_c_database.data,
                biview_database=form_update.biview_database.data,
                biview_project_type=form_update.biview_project_type.data,
                ct_database=form_update.ct_database.data,
                transfer_source_data=form_update.transfer_source_data.data,
                target_connection_id=form_update.target_connection_id.data,
                target_database=form_update.target_database.data,
                target_type=form_update.target_type.data,
                ct_project_name=form_update.ct_project_name.data
            )

            sql_update_query = f"""
                                UPDATE airflow.atk_ct.ct_projects
                                SET ct_project_name = '{my_task_output.ct_project_name}',
                                    source_connection_id = '{my_task_output.source_connection_id}',
                                    one_c_database = '{my_task_output.one_c_database}',
                                    biview_database = '{my_task_output.biview_database}',
                                    biview_project_type = {my_task_output.biview_project_type},
                                    transfer_source_data = {my_task_output.transfer_source_data},
                                    target_connection_id = '{my_task_output.target_connection_id}',
                                    target_database = '{my_task_output.target_database}',
                                    target_type = '{my_task_output.target_type}'
                                WHERE ct_project_id = '{projects_data['ct_project_id']}'
                                ;"""
            print(sql_update_query)
            try:
                with get_connection_postgres().get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_update_query)
                    conn.commit()

                flash("Проект успешно измененн", category="info")
                return self.render_template("edit_project.html", form=form_update)

            except Exception as e:
                if 'duplicate key' in str(e):
                    flash("Данное имя проекта уже существует! Выберите другое.", category='warning')
                else:
                    flash(str(e), category='warning')
                return self.render_template("edit_project.html", form=form_update)

        return self.render_template("edit_project.html", form=form_existing)

    @expose('/projects_to_load', methods=['GET'])
    def projects_to_load(self):
        """Render a new HTML page"""
        return self.render_template("projects_to_load.html")

    @expose('/delete/<int:ct_project_id>', methods=['GET'])
    @csrf.exempt
    def delete_ct_project(self, ct_project_id):
        """Удаление проекта"""
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
