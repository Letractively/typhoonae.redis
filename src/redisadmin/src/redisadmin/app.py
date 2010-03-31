"""Sample application for Google App Engine and TyphoonAE."""

import google.appengine.ext.webapp
import google.appengine.ext.webapp.template
import google.appengine.ext.webapp.util
import wsgiref.handlers


class MainRequestHandler(google.appengine.ext.webapp.RequestHandler):
    """The main request handler."""

    def get(self):
        """Handles GET."""

        vars = {}

        self.response.out.write(
            google.appengine.ext.webapp.template.render('index.html', vars))


app = google.appengine.ext.webapp.WSGIApplication([
    ('/', MainRequestHandler), 
], debug=True)


def main():
    """The main function."""

    google.appengine.ext.webapp.util.run_wsgi_app(app)


if __name__ == '__main__':
    main()
