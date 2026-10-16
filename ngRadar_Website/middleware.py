from django.db import connection, OperationalError, DatabaseError
from django.http import HttpResponse


class DatabaseUnavailableMiddleware:

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):

        try:
            connection.ensure_connection()

        except (OperationalError, DatabaseError) as e:
            print(f"DATABASE ERROR: {e}")

            return HttpResponse(
                """
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Database Unavailable</title>
                </head>
                <body>
                    <h1> Database Unavailable</h1>
                    <p>
                        The database is currently unavailable.
                        Please try again later.
                    </p>
                </body>
                </html>
                """,
                status=503,
                content_type="text/html",
            )
        except Exception as e:
            # If it tries to connect to the DB and hits this error, the DB is not available
            print(f"INTERNAL SERVER ERROR: {e}")

            return HttpResponse(
                """
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Internal Server Error</title>
                </head>
                <body>
                    <h1>Internal Server Error</h1>
                    <p>
                        The database is currently unavailable.
                        Please try again later.
                    </p>
                </body>
                </html>
                """,
                status=500,
                content_type="text/html",
            )

        return self.get_response(request)