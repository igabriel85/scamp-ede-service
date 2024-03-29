from service import *
import os

host = os.environ.get('EDE_HOST', '0.0.0.0')
port = os.environ.get('EDE_PORT', 5001)
debug = os.environ.get('EDE_DEBUG', True)
if __name__ == '__main__':
  try:
    import bjorn
    bjorn.run(
      host=host,
      port=port,
      debug=debug)
  except ImportError:
    app.run(
      host=host,
      port=port,
      debug=debug)
