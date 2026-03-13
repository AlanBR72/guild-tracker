import requests

url = "https://www.rucoyonline.com/characters/Ws%20Pato"

r = requests.get(url)

print(r.status_code)
print(r.text)
