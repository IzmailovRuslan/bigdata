import os
import subprocess

timeout = 120

kafka = subprocess.Popen(['docker-compose', 'up'])
client = subprocess.Popen(['streamlit', 'run', 'app.py'])
data_get = subprocess.Popen(['python', 'server/interactions_producer.py'])

try:
    kafka.wait(timeout)
    client.wait(timeout)
    data_get.wait(timeout)
except:
    kafka.kill()
    client.kill()
    data_get.kill()