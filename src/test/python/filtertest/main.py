from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import db

class ReferenceModel(db.Model):
    pass

class TestModel(db.Model):
    area = db.ReferenceProperty(ReferenceModel)
    created = db.DateTimeProperty(auto_now_add=True)
    is_root = db.BooleanProperty(default=True)
    is_active = db.BooleanProperty(default=True)

def populate_datastore():
    i = 0
    while i < 10:
        entity = TestModel()
        entity.put()
        i += 1

class MainPage(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'

        res = TestModel.all().order('-created').fetch(3)

        if not res:
            populate_datastore()

        res = TestModel.all().order('-created').fetch(3)
        self.response.out.write('%d results with no filters.\n' % len(res))

        res = TestModel.all().filter('is_root =', True).order('-created').fetch(3)
        self.response.out.write('%d results with a filter.\n' % len(res))


application = webapp.WSGIApplication([('/', MainPage)], debug=True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
