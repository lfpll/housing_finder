from asyncio import Queue

class Proxy_rotate:

	def __init__(self, proxys):
		self.proxys = proxys
		self.q_proxys = Queue()

	def load_queue(self):
		[self.q_proxys.put_nowait(proxy) for proxy in self.proxys]

	def get_proxy(self):
		if self.q_proxys.empty():
			self.load_queue()
		return self.q_proxys.get()

	def remove_value(self,proxy):
		if proxy in self.proxys:
			self.proxys.remove(proxy)
