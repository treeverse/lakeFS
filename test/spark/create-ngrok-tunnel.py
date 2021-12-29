from pyngrok import ngrok
from urllib.parse import urlparse
import tempfile
import os


if __name__ == "__main__":
    http_tunnel = ngrok.connect("8000", "http")
    url = http_tunnel.public_url
    parsed_url = urlparse(url)
    domain = parsed_url.hostname
    with open('./.ngrok_domain', 'w+') as f:
        f.write(domain)
    with tempfile.NamedTemporaryFile(mode='w+', dir=".") as f:
        f.write(url)
        f.flush()
        os.fsync(f.fileno())
        os.link(src=f.name, dst='./.ngrok_url')
        f.close()
    ngrok_process = ngrok.get_ngrok_process()
    ngrok_process.proc.wait()
