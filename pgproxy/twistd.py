from twisted.scripts.twistd import ServerOptions, runApp
from twisted.application import app


class Options(ServerOptions):
    optParameters = [
        ('listen-port', '', 5433, 'The port to listen on.', int),
        ('server-host', '', 'localhost', 'The host of the postgres server.'),
        ('server-port', '', 5432, 'The port of the postgres server.', int),
        ]


def run():
    app.run(runApp, Options)


if __name__ == '__main__':
    run()
