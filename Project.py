import requests
import json
import matplotlib.pyplot as plt

vpic_api_url = "https://vpic.nhtsa.dot.gov/api/vehicles/GetMakesForVehicleType/car?format=json"
vpic_response = requests.get(vpic_api_url)
vpic_data = json.loads(vpic_response.text)['Results']

carquery_api_url = "https://www.carqueryapi.com/api/0.3/?cmd=getMakes&year=2015"
carquery_response = requests.get(carquery_api_url)
carquery_data = json.loads(carquery_response.text)['Makes']

vpic_makes = [d['Make_Name'] for d in vpic_data]
carquery_makes = [d['make_display'] for d in carquery_data]