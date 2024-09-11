from airflow.hooks.base import BaseHook
from flask import Blueprint, jsonify, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from airflow.models import Connection
from airflow.utils.session import provide_session
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.www.app import csrf
import os


my_blueprint = Blueprint(
    "mssql_plugin",
    __name__,
    template_folder="templates",
    static_folder="static",  # Reference to the folder inside 'plugins'
    static_url_path="/static/mssql_plugin"
)

class MyBaseView(AppBuilderBaseView):
    default_view = "test"
  
    @expose("/")
    def test(self):
        return self.render_template("test.html")
    
    @expose("/fetch_airflow_connections")
    @provide_session
    def fetch_airflow_connections(self, session=None):
        try:
            connections = session.query(Connection).all()
            connection_ids = [conn.conn_id for conn in connections]
            return jsonify({"status": "success", "connections": connection_ids})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)})
        
    @expose("/fetch_data")
    def fetch_data(self):
        project_id = request.args.get('project_id')
        
        print("*" * 20)
        print("project_id: ", project_id)
        print("*" * 20)
        
        pg_hook = PostgresHook(postgres_conn_id='airflow_postgres')
        pg_connection = pg_hook.get_conn()
        pg_cursor = pg_connection.cursor()
        pg_cursor.execute("SELECT * FROM atk_ct.ct_tables WHERE project_id=%s ORDER BY table_name;", (project_id, ))
        pg_results = pg_cursor.fetchall()
        pg_columns = [desc[0] for desc in pg_cursor.description]

        pg_cursor.close()
        pg_connection.close()

        # Prepare data for the template
        projects = [dict(zip(pg_columns, row)) for row in pg_results]

        response_data = {
            "status": "success",
            "columns": pg_columns,
            "results": projects
        }

        return jsonify(response_data)

    @expose("/update_and_fetch_data")
    def update_and_fetch_data(self):
        connection_id = request.args.get('connection')
        project_id = request.args.get('project_id')
        biview_database = request.args.get('biview_database')
        print(" * " * 20)
        print("biview_database:", biview_database)
        print(" * " * 20)
        if not connection_id:
            return jsonify({"status": "error", "message": "No connection selected"})

        if not biview_database:
            return jsonify({"status": "error", "message": "No biview_database selected"})

        try:
            # MSSQL Hook to fetch table names
            mssql_hook = MsSqlHook(mssql_conn_id=connection_id)
            mssql_query = f"""
                SELECT 
                    TABLE_NAME
                FROM 
                    {biview_database}.INFORMATION_SCHEMA.TABLES
                WHERE 
                    TABLE_TYPE = 'BASE TABLE' AND 
                    TABLE_SCHEMA = 'dbo' AND 
                    TABLE_NAME LIKE 'ATK_%'
                
                UNION ALL
                
                SELECT 
                    [name]
                FROM 
                    {biview_database}.sys.objects o
                WHERE 
                    [type] IN ('V')
                ORDER BY 1;
            """


            mssql_connection = mssql_hook.get_conn()
            mssql_cursor = mssql_connection.cursor()
            mssql_cursor.execute(mssql_query)
            mssql_results = mssql_cursor.fetchall()
            mssql_cursor.close()
            mssql_connection.close()
            print("connection is closed")

            table_names = [row[0] for row in mssql_results]
            print(" * " * 20)
            print("table_names:", table_names)
            
            # PostgreSQL Hook to insert data
            pg_hook = PostgresHook(postgres_conn_id='airflow_postgres')
            pg_connection = pg_hook.get_conn()
            pg_cursor = pg_connection.cursor()

            for table_name in table_names:
                try:
                    pg_cursor.execute(
                        """
                        INSERT INTO atk_ct.ct_tables (project_id, table_name, load) 
                        VALUES (%s, %s, %s)
                        ON CONFLICT (project_id, table_name) 
                        DO NOTHING;
                        """,
                        (project_id, table_name, True)
                    )
                except Exception as e:
                    print(f"Exception for table {table_name}: {e}")
                    continue 


            pg_connection.commit()

            # Fetch data from atk_ct table
            pg_cursor.execute("SELECT * FROM atk_ct.ct_tables WHERE project_id=%s;", (project_id, ))
            pg_results = pg_cursor.fetchall()
            pg_columns = [desc[0] for desc in pg_cursor.description]

            pg_cursor.close()
            pg_connection.close()

            # Prepare data for the template
            projects = [dict(zip(pg_columns, row)) for row in pg_results]

            response_data = {
            "status": "success",
            "columns": pg_columns,
            "results": projects
            }

            return jsonify(response_data)
            
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)})

    @expose("/update_data_is_load", methods=['POST'])
    @csrf.exempt
    def update_data_is_load(self):
        try:
            data = request.get_json()
            print("$" * 20)
            print("data: ", data)  # This should now include 'project_id' and 'data'
            print("$" * 20)

            project_id = data.get('project_id')  # Extract project_id
            changes = data.get('data')  # Extract the actual data array

            if not project_id or not changes:
                return jsonify({'status': 'error', 'message': 'Missing project_id or data'}), 400

            # PostgreSQL Hook to connect to the database
            pg_hook = PostgresHook(postgres_conn_id='airflow_postgres')
            pg_connection = pg_hook.get_conn()
            pg_cursor = pg_connection.cursor()

            try:
                update_queries = []
                for entry in changes:
                    table_name = entry['table_name']
                    for change in entry['changes']:
                        field = change['field']
                        new_value = change['newValue']

                        # Use project_id in the SQL query
                        query = f"""
                        UPDATE atk_ct.ct_tables
                        SET {field} = %s
                        WHERE table_name = %s AND project_id = %s;
                        """
                        update_queries.append((query, (new_value, table_name, project_id)))
                
                # Execute the queries
                for query, params in update_queries:
                    pg_cursor.execute(query, params)
                pg_connection.commit()

            except Exception as e:
                pg_connection.rollback()  # Rollback in case of error
                print(f"Error occurred while updating data: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500

            finally:
                pg_cursor.close()
                pg_connection.close()

            return jsonify({'status': 'success'}), 200

        except Exception as e:
            print(f"Error processing request: {e}")
            return jsonify({'status': 'error', 'message': str(e)}), 500

    
my_view = MyBaseView()

class MyViewPlugin(AirflowPlugin):
    name = "My appbuilder view"
    flask_blueprints = [my_blueprint]
    appbuilder_views = [
        {
            # "name": "MSSQL View",
            # "category": "MSSQL table view",
            "view": my_view,
        }
    ]
