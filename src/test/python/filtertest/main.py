from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import db
import logging

class ReferenceModel(db.Model):
    id = db.StringProperty(default='1')
    sf = db.FloatProperty(default=100.0)

class TestModel(db.Model):
    reference = db.ReferenceProperty(ReferenceModel)
    created = db.DateTimeProperty(auto_now_add=True)
    is_root = db.BooleanProperty(default=True)
    is_active = db.BooleanProperty(default=True)
    tags = db.StringListProperty(default=['1','2','3'])
    id = db.StringProperty(default='1')

def populate_datastore(count=10):
    i = 0
    while i < count:
        # using keynames, let's us overwrite the same entities, over and over
        id = 'ReferenceModel:%s' % i
        ref = ReferenceModel(key_name=id)
        ref.id = id
        ref.put()
        logging.debug('ReferenceModel:%s' % ref._entity)
        tid = 'TestModel:%s' % i
        entity = TestModel(key_name=tid)
        entity.id = tid
        entity.reference = ref
        entity.put()
        logging.debug('TestModel:%s' % entity._entity)
        i += 1

# to test with these filters

# export PYTHONPATH=.:google_appengine:google_appengine/lib/antlr3:google_appengine/lib/webob:google_appengine/lib/yaml/lib

# startup a server, normal dev_server, or bdbdatastore one

#python google_appengine/dev_appserver.py src/test/python/filtertest/ -p 8001 --bdbdatastore=localhost --require_indexes

#python google_appengine/dev_appserver.py src/test/python/filtertest/ -p 8001 --clear_datastore 

# then execute the test with a client access
# wget --timeout=0 http://localhost:8001

class MainPage(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'

        populate_datastore()

        ref = ReferenceModel.all().filter('id =','ReferenceModel:3').fetch(1)[0]
        res = TestModel.all().filter('id =','BadId').filter('reference =',
                                      ref).fetch(3)
        logging.info('%s results non existent id and key.\n' % (len(res) == 0))
        self.response.out.write(
            '%d results non existent id and key.\n' % len(res))

        res = TestModel.all().order('-created').fetch(100)
        logging.info('%s results with no filters.\n' % (len(res) == 10))
        self.response.out.write('%d results with no filters.\n' % len(res))

        res = TestModel.all().filter('is_root =',
                                     True).order('-created').fetch(100)
        logging.info('%s results with boolean filters.\n' % (len(res) == 10))
        self.response.out.write('%d results with a boolean filter.\n' % len(res))
        ref = ReferenceModel.all().filter('id =','ReferenceModel:3').fetch(1)[0]
        res = TestModel.all().filter('id =','TestModel:3').filter('reference =',
                                      ref).fetch(3)
        logging.info('%s result string and key.\n' % (len(res) == 1))
        self.response.out.write( '%d result string and key.\n' % len(res))

        ref = ReferenceModel.all().filter('id =','ReferenceModel:3').fetch(1)[0]
        res = TestModel.all().filter('tags =','3').filter('reference =',
                                      ref).fetch(3)
        logging.info('%s result stringList and key.\n' % (len(res) == 1))
        self.response.out.write( '%d result stringList and key.\n' % len(res))

application = webapp.WSGIApplication([('/', MainPage)], debug=True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
