import os
import subprocess

kafka = subprocess.Popen(['docker-compose', 'up'])
client = subprocess.Popen(['streamlit', 'run', 'app.py'])
data_get = subprocess.Popen(['python', 'server/interactions_producer.py'])

try:
    kafka.wait(120)
    client.wait(120)
    data_get.wait(120)
except:
    kafka.kill()
    client.kill()
    data_get.kill()