from flask import Flask, render_template, request, redirect, url_for, flash
from sqlalchemy import create_engine, MetaData, inspect, text
from sqlalchemy.exc import SQLAlchemyError

app = Flask(__name__)
app.secret_key = 'mymegasecretkey' # change for production


def create_db_connection(db_data):
    try:
        engine = create_engine(
            f"{db_data['db_type']}://{db_data['username']}:{db_data['password']}@"
            f"{db_data['host']}:{db_data['port']}/{db_data['database_name']}")
        return engine
    except SQLAlchemyError as e:
        flash(f"Error connecting to database: {str(e)}", "danger")
        return None


def copy_tables(from_engine, to_engine):
    try:
        source_metadata = MetaData()
        with from_engine.connect() as source_conn:
            source_metadata.reflect(bind=source_conn)

        for table in source_metadata.sorted_tables:
            table.metadata = MetaData()
            table.create(to_engine, checkfirst=True)  # Create table in target DB if it doesn't exist

            # Copy data
            with from_engine.connect() as source_conn, to_engine.connect() as target_conn:
                rows = source_conn.execute(table.select()).fetchall()

                if rows:
                    data_to_insert = [{col.name: value for col, value in zip(table.columns, row)} for row in rows]

                    target_conn.execute(table.insert(), data_to_insert)
                    target_conn.commit()

            # Check seq and change to max
            primary_key_column = table.primary_key.columns.values()[0]
            if primary_key_column.autoincrement:
                with to_engine.connect() as target_conn:
                    max_id = target_conn.execute(
                        text(f"SELECT MAX({primary_key_column.name}) FROM {table.name}")
                    ).scalar()

                    # Adjust the sequence to start from max_id + 1 if necessary
                    if max_id is not None:
                        if to_engine.dialect.name == 'postgresql':
                            sequence_name = f"{table.name}_{primary_key_column.name}_seq"
                            target_conn.execute(
                                text(f"SELECT setval('{sequence_name}', :new_start, false)"),
                                {"new_start": max_id + 1}
                            )
                        elif to_engine.dialect.name == 'mysql':
                            target_conn.execute(
                                text(f"ALTER TABLE {table.name} AUTO_INCREMENT = :new_start"),
                                {"new_start": max_id + 1}
                            )

        flash("Tables and data copied successfully, including sequence adjustments!", "success")
    except Exception as e:
        flash(f"Error copying tables and data: {str(e)}", "danger")

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/copy', methods=['POST'])
def copy():
    # Get 'From Database' inputs
    from_db_data = {
        'db_type': request.form.get('from_db_type'),
        'host': request.form.get('from_host'),
        'port': request.form.get('from_port'),
        'username': request.form.get('from_username'),
        'password': request.form.get('from_password'),
        'database_name': request.form.get('from_database')
    }

    # Get 'To Database' inputs
    to_db_data = {
        'db_type': request.form.get('to_db_type'),
        'host': request.form.get('to_host'),
        'port': request.form.get('to_port'),
        'username': request.form.get('to_username'),
        'password': request.form.get('to_password'),
        'database_name': request.form.get('to_database')
    }

    from_engine = create_db_connection(from_db_data)
    to_engine = create_db_connection(to_db_data)

    if from_engine and to_engine:
        copy_tables(from_engine, to_engine)

    return redirect(url_for('index'))


if __name__ == '__main__':
    app.run(debug=False)
