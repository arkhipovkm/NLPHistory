import requests
user = 'p1616483@univ-lyon1.fr'
passwd = 'Sicherheit3'
url = 'https://gtwresc.insa-lyon.fr:8003/index.php?zone=dsi_capt'
data = {'auth_user': user, 'auth_pass': passwd, 'accept': 'Login'}
resp = requests.post(url, data=data)