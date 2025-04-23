import requests
import json
import matplotlib.pyplot as plt

crash_api_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCaseList?State=NY&Year=2021&format=json"
crash_response = requests.get(crash_api_url)
crash_data = json.loads(crash_response.text)['Results']

carquery_api_url = "https://www.carqueryapi.com/api/0.3/?cmd=getMakes&year=2015"
carquery_response = requests.get(carquery_api_url)
carquery_data = json.loads(carquery_response.text)['Makes']

vpic_makes = [d['Make_Name'] for d in crash_data]
carquery_makes = [d['make_display'] for d in carquery_data]

