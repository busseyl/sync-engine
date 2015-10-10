import subprocess


def setup_test_db():
    """
    Creates a new, empty test database with table structure generated
    from declarative model classes; returns an engine for that database.

    """
    from inbox.ignition import engine_manager
    from inbox.ignition import init_db
    engine = engine_manager.get_for_id(0)
    db_invocation = 'DROP DATABASE IF EXISTS test; ' \
                    'CREATE DATABASE IF NOT EXISTS test ' \
                    'DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE ' \
                    'utf8mb4_general_ci'

    subprocess.check_call('mysql -uinboxtest -pinboxtest '
                          '-e "{}"'.format(db_invocation), shell=True)
    init_db(engine)
    return engine
