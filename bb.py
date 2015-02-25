import gevent
import zerorpc
endpoint = "tcp://0.0.0.0:9191"

class MySrv(zerorpc.Server):
	def lolita(self):
		return 42
srv = MySrv()
srv.bind(endpoint)
srv.run()