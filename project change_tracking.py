import json
import pandas as pd
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request, jsonify, url_for, redirect, flash
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from wtforms import Form, SelectField, RadioField, DateField, StringField
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
    def __init__(self, project_name, connection, project_type, load_type, database_source, database_change_tracking,
                 start, schedule):
        self.project_name = project_name
        self.connection = connection
        self.project_type = project_type
        self.load_type = load_type
        self.database_source = database_source
        self.database_change_tracking = database_change_tracking
        self.start = start
        self.schedule = schedule

    def jsonify_data(self) -> str:
        """Transformation data in json format"""
        result = {
            'project_name': self.project_name,
            'connection': self.connection,
            'project_type': self.project_type,
            'load_type': self.load_type,
            'database_source': self.database_source,
            'database_change_tracking': self.database_change_tracking,
            'start': str(self.start),
            'schedule': self.schedule
        }
        return json.dumps(result, indent=4)


class FilterForm(Form):
    """Форма получения данных об используемых коннектах и базах данных"""

    project_name = StringField('CT Project ID',
                               validators=[InputRequired()],
                               id="conn_id",

                               render_kw={"placeholder": "Введите имя проекта",
                                          "class": "form-control",
                                          "type": "text"
                                          }
                               )

    connection = SelectField('Connection ID',
                             choices=get_all_connections(),
                             id="conn_type",
                             render_kw={"class": "form-control",
                                        "data-placeholder": "Select Value",
                                        },
                             )

    load_type = SelectField('Source Connection ID',
                            choices=['CDC', 'HODS', 'CT'],
                            id="conn_type",
                            name="conn_type1",
                            render_kw={"class": "form-control",
                                       "data-placeholder": "Select Value",
                                       },
                            )

    project_type = RadioField('Тип проекта',
                              validators=[InputRequired()],
                              choices=[('1', 'Type 1'), ('2', 'Type 2')],
                              default='1',
                              name="project_type",
                              id="project_type",
                              render_kw={"class": "form-check-input",
                                         "type": "radio"
                                         }
                              )

    database_source = SelectField('ДБ - источник',
                                  choices=get_all_database_mssql(),
                                  id="conn_type",
                                  name="conn_type2",
                                  render_kw={"class": "form-control",
                                             "data-placeholder": "Select Value"
                                             }
                                  )

    database_change_tracking = SelectField('ДБ - change',
                                           choices=get_all_database_mssql(),
                                           id="conn_type",
                                           name="conn_type3",
                                           render_kw={"class": "form-control",
                                                      "data-placeholder": "Select Value"
                                                      }
                                           )

    start = DateField('Дата Начала',
                      validators=[InputRequired()],
                      render_kw={"class": "form-control-short"}
                      )

    schedule = StringField('Расписание',
                           validators=[InputRequired(), validate_cron],
                           default="* * * * *",
                           render_kw={"class": "form-control-short",
                                      "data-placeholder": "Select Value"
                                      }
                           )


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

        form = FilterForm(request.form)

        if request.method == 'POST':
            my_task_output = MyTask(
                project_name=form.project_name.data,
                connection=form.connection.data,
                project_type=form.project_type.data,
                load_type=form.load_type.data,
                database_source=form.database_source.data,
                database_change_tracking=form.database_change_tracking.data,
                start=form.start.data,
                schedule=form.schedule.data,
            )

            sql_insert_query = f"""
                                INSERT INTO airflow.atk_ct.ct_projects (
                                    project_name,
                                    connection_id,
                                    project_type,
                                    load_type,
                                    db_source,
                                    db_target,
                                    start_date,
                                    schedule)
                                VALUES (
                                    '{my_task_output.project_name}',
                                    '{my_task_output.connection}',
                                    {my_task_output.project_type},
                                    '{my_task_output.load_type}',
                                    '{my_task_output.database_source}',
                                    '{my_task_output.database_change_tracking}',
                                    '{my_task_output.start}',
                                    '{my_task_output.schedule}'
                                    );"""
            print(sql_insert_query)
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

    @expose("/edit/<int:project_id>", methods=['GET', 'POST'])
    @csrf.exempt
    def edit_project_data(self, project_id):
        """Редактирование данных проекта"""

        print("Im in edit_project_data")
        
        sql_select_query = f"""SELECT * FROM airflow.atk_ct.ct_projects WHERE project_id = '{project_id}';"""

        with get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_select_query)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                projects_data = [dict(zip(columns, row)) for row in rows][0]

        projects_data['start_date'] = projects_data['start_date'].strftime('%d.%m.%Y')
        print(projects_data)

        form_existing = FilterForm(data=projects_data)

        if request.method == 'POST':
            form_update = FilterForm(request.form)
            my_task_output = MyTask(
                project_name=form_update.project_name.data,
                connection=form_update.connection.data,
                project_type=form_update.project_type.data,
                load_type=form_update.load_type.data,
                database_source=form_update.database_source.data,
                database_change_tracking=form_update.database_change_tracking.data,
                start=form_update.start.data,
                schedule=form_update.schedule.data
            )

            sql_insert_query = f"""
                                UPDATE airflow.atk_ct.ct_projects cp
                                SET project_name = '{my_task_output.project_name}',
                                    connection_id = '{my_task_output.connection}',
                                    project_type = {my_task_output.project_type},
                                    load_type = '{my_task_output.load_type}',
                                    db_source = '{my_task_output.database_source}',
                                    db_target = '{my_task_output.database_change_tracking}',
                                    start_date = '{my_task_output.start}',
                                    schedule = '{my_task_output.schedule}'
                                WHERE cp.project_id = '{projects_data['project_id']}'    
                                    ;"""
            print(sql_insert_query)
            try:
                with get_connection_postgres().get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_insert_query)
                    conn.commit()

                flash("Проект успешно измененн", category="info")
                return self.render_template("add_projects.html", form=form_update)

            except Exception as e:
                if 'duplicate key' in str(e):
                    flash("Данное имя проекта уже существует! Выберите другое.", category='warning')
                else:
                    flash(str(e), category='warning')
                return self.render_template("add_projects.html", form=form_update)

        return self.render_template("add_projects.html", form=form_existing)

    @expose('/projects_to_load', methods=['GET'])
    def projects_to_load(self):
        """Render a new HTML page"""
        
        project_name = request.args.get('project_name')
        connection = request.args.get('connection')
        return self.render_template("projects_to_load.html", project_name=project_name, connection=connection)
    
    


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
