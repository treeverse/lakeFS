from pyngrok import ngrok
import tempfile
import os
if __name__ == "__main__":
    http_tunnel = ngrok.connect("8000", "http")
    with tempfile.NamedTemporaryFile(mode='w+', dir=".") as f:
        f.write(http_tunnel.public_url)
        f.flush()
        os.fsync(f.fileno())
        os.link(src=f.name, dst='./.ngrok_url')
        f.close()
    ngrok_process = ngrok.get_ngrok_process()
    ngrok_process.proc.wait()
