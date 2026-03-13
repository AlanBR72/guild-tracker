import requests
from bs4 import BeautifulSoup

url = "https://www.rucoyonline.com/guild/Guilt%20Of%20Virtue"
r = requests.get(url)
print(r.status_code)  # deve ser 200
print(r.text[:500])   # imprime os primeiros 500 caracteres do HTML
