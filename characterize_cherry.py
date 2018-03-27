from cherrypy import expose, response, request, quickstart, config, popargs, tree
from nlpdb import DB


@popargs('id')
class Characterize():

    @expose
    def characterize(self, id):
        with DB() as db:
            db.custom_get('select * from rubrics')