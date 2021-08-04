import psycopg2


def on_connection(*, db_info):
    def decorator(func):
        def wrapper(**kwargs):
            connection = psycopg2.connect(database=db_info.get('NAME'),
                                          user=db_info.get('USER'),
                                          password=db_info.get('PASSWORD'),
                                          host=db_info.get('HOST'),
                                          port=db_info.get('PORT'))
            res = func(**kwargs, conn=connection)
            connection.close()
            return res
        return wrapper
    return decorator
