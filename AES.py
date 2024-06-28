from reactor_usage import Reactor
rct = Reactor('85.209.90.29:5000')
rs = rct.full_proxy(proxy_type='private')
print(rs)