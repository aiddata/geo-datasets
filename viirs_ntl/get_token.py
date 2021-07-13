import requests
import json

user = '<username>'
pw = '<password>'

# Retrieve access token
params = {
    'client_id' : 'eogdata_oidc',
    'client_secret' : '368127b1-1ee0-4f3f-8429-29e9a93daf9a',
    'username' : user,
    'password' : pw,
    'grant_type' : 'password'
}

token_url = 'https://eogauth.mines.edu/auth/realms/master/protocol/openid-connect/token'
response = requests.post(token_url, data = params)
access_token_dict = json.loads(response.text)
access_token = access_token_dict.get('access_token')

print(access_token)

