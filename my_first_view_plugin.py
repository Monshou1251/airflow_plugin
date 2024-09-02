from airflow.hooks.base import BaseHook
from flask import Blueprint, jsonify, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from airflow.models import Connection
from airflow.utils.session import provide_session
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.www.app import csrf


my_blueprint = Blueprint(
    "mssql_plugin",
    __name__,
    template_folder="templates",
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
        connection_id = request.args.get('connection')
        if not connection_id:
            return jsonify({"status": "error", "message": "No connection selected"})

        try:
            # MSSQL Hook to fetch table names
            mssql_hook = MsSqlHook(mssql_conn_id=connection_id)
            mssql_query = """
                SELECT
                    TABLE_NAME
                FROM 
                    BU83_BIVIEW1__ct.INFORMATION_SCHEMA.TABLES
                WHERE 
                    TABLE_TYPE = 'BASE TABLE' AND 
                    TABLE_SCHEMA = 'dbo';
            """
            mssql_connection = mssql_hook.get_conn()
            mssql_cursor = mssql_connection.cursor()
            mssql_cursor.execute(mssql_query)
            mssql_results = mssql_cursor.fetchall()
            mssql_cursor.close()
            mssql_connection.close()

            table_names = [row[0] for row in mssql_results]
            
            # PostgreSQL Hook to insert data
            pg_hook = PostgresHook(postgres_conn_id='airflow_postgres')
            pg_connection = pg_hook.get_conn()
            pg_cursor = pg_connection.cursor()

            for table_name in table_names:
                pg_cursor.execute(
                    "INSERT INTO atk_ct.ct_tables (table_name, load) VALUES (%s, %s) ON CONFLICT (table_name) DO NOTHING;",
                    (table_name, True)
                )

            pg_connection.commit()

            # Fetch data from atk_ct table
            pg_cursor.execute("SELECT * FROM atk_ct.ct_tables;")
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
            if not data:
                return jsonify({'status': 'error', 'message': 'No data provided'}), 400
            
            # PostgreSQL Hook to connect to the database
            pg_hook = PostgresHook(postgres_conn_id='airflow_postgres')
            pg_connection = pg_hook.get_conn()
            pg_cursor = pg_connection.cursor()

            try:
                update_queries = []
                for entry in data:
                    print("entry")
                    print(entry)
                    table_name = entry['table_name']
                    for change in entry['changes']:
                        print("change")
                        print(change)
                        field = change['field']
                        new_value = change['newValue']
                        
                        # SQL Injection Mitigation: Use placeholders for values
                        query = f"""
                        UPDATE atk_ct.ct_tables
                        SET {field} = %s
                        WHERE table_name = %s;
                        """
                        update_queries.append((query, (new_value, table_name)))
                print("update_queries")
                print(update_queries)
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
            "name": "MSSQL View",
            "category": "MSSQL table view",
            "view": my_view,
        }
    ]
