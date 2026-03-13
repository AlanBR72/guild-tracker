import requests

url = "https://www.rucoyonline.com/characters/Alan%20Virtue"

r = requests.get(url)

print(r.status_code)
print(r.text)
