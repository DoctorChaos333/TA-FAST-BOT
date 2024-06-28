import requests, subprocess
key = "8UsxjmY7xk9UNbrQ"
current_machine_id = str(subprocess.check_output('wmic csproduct get uuid'), 'utf-8').split('\n')[1].strip()
data = {'key': key, 'current_machine_id': current_machine_id}
response = requests.post('http://185.195.24.244:1337/api', json=data)
print(response.json())