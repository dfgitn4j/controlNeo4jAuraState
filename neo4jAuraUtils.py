import requests
import time
from datetime import datetime
import pandas as pd
import configparser
import os

# function adapted from Neo4j GDS Fraud Demo Notebook (h/t Zach B.)
def read_neo4j_properties(NEO4J_PROPERTIES_FILE: str = None) -> tuple:
    '''Parses Neo4j database or Aura connection details from provided .ini filepath.
        Requirements:
            import
                configparser

            Aura
                Get api client id and generate secret key from your account section of the Aura console at
                https://console.neo4j.io/#account

        Args:
            NEO4J_PROPERTIES_FILE: path to a .ini file

        .ini config
            file section is [AURA]

        Returns:
            AURA_URL = "neo4j+s://<instance id>.databases.neo4j.io"
            AURA_API = https://api.neo4j.io/v1/instances/
            AURA_TOKEN_URL = https://api.neo4j.io/oauth/token
            AURA_API_CLIENT_ID = <generated string>
            AURA_CLIENT_SECRET = <generated string>
    '''

    if NEO4J_PROPERTIES_FILE is not None and os.path.exists(NEO4J_PROPERTIES_FILE):
        config = configparser.RawConfigParser()
        config.read(NEO4J_PROPERTIES_FILE)
        aura_api = config['AURA']['AURA_API']
        aura_url = config['AURA']['AURA_URL']
        aura_token_url = config['AURA']['AURA_TOKEN_URL']
        aura_api_client_id = config['AURA']['AURA_API_CLIENT_ID']
        aura_client_secret = config['AURA']['AURA_CLIENT_SECRET']
        print(f"""Using AURA_API, AURA_URL, AURA_TOKEN_URL, AURA_API_CLIENT_ID, AURA_CLIENT_SECRET from {NEO4J_PROPERTIES_FILE} file""")
        return aura_api, aura_url, aura_token_url, aura_api_client_id, aura_client_secret
    else:  # assume using local database
        print("Awww... Snap!\nERROR: Could not find database properties file: {NEO4J_PROPERTIES_FILE} ")


def aura_set_request_header(aura_api_client_id, aura_client_secret, aura_token_url) -> dict:
    # setup request
    data = {'grant_type': 'client_credentials'}

    # get aura access token

    r = requests.post(
        aura_token_url,
        data=data,
        auth=(aura_api_client_id, aura_client_secret))

    return {
        'Authorization': 'Bearer ' + r.json()['access_token'],
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }


def aura_inst_id(aura_url) -> str:
    # pull Aura instance id from the connection URI
    # The instance id is '2bxxxxxx' in the connection string neo4j+s://2bxxxxxx.databases.neo4j.io
    start = aura_url.find("//")
    end = aura_url.find(".")
    inst_id = aura_url[start + len('//'): end]

    return inst_id


def aura_inst_info(aura_api, inst_id, headers) -> dict:
    r = requests.get(aura_api + inst_id, headers=headers)
    inst_info = r.json()['data']
    inst_info['aura_api'] = aura_api
    inst_info['headers'] = headers  # add header data for future calls to inst_info
    inst_info['info_updated'] = datetime.now().isoformat(' ', 'seconds')

    return inst_info


def aura_inst_state(inst_info) -> dict:
    r = requests.get(inst_info['aura_api'] + inst_info['id'], headers=inst_info['headers'])
    inst_info['status'] = r.json()['data']['status']
    inst_info['info_updated'] = datetime.now().isoformat(' ', 'seconds')
    return inst_info


def aura_change_state(new_state, inst_action, inst_info: dict):
    req_start_time = datetime.now()

    print("%s : Requesting changing Aura instances for instance '%s' (id: '%s') state from '%s' to '%s'" % (
        req_start_time.isoformat(' ', 'seconds'), inst_info['name'], inst_info['id'], inst_info['status'], new_state),
          end="")
    r = requests.post(inst_info['aura_api'] + inst_info['id'] + "/" + inst_action, headers=inst_info['headers'])

    while new_state != aura_inst_state(inst_info)['status']:
        print(".", end="")
        time.sleep(5)

    req_end_time = datetime.now()
    print(f"\n{req_end_time.isoformat(' ', 'seconds')} : Instance state for instance '{inst_info['name']}' (id: '{inst_info['id']}') is now '{new_state}'")
    mm, ss = divmod((req_end_time - req_start_time).seconds, 60)
    hh, mm = divmod(mm, 60)
    print(f"Elapsed time to change state {hh:0>{2}}:{mm:0>{2}}:{ss:0>{2}}")
    return


def aura_request_state_change(new_state, inst_info):
    inst_info = aura_inst_state(inst_info)

    if inst_info['status'] == new_state:
        print("Instance '%s' (id: '%s') is already '%s'." % (inst_info['name'], inst_info['id'], inst_info['status']))
    elif new_state == "running" and inst_info['status'] == "paused":
        # put in checks to see if gds or cypher queries are running before allowing shutdown.
        aura_change_state(new_state, "resume", inst_info)
    elif new_state == "paused" and inst_info['status'] == "running":
        # make sure that nobody is running anything. it's not in the code
        aura_change_state(new_state, "pause", inst_info)
    else :     # only accept certain combinations of pausing and running
        print("Can only change instance state from 'running' to 'paused', or 'paused' to 'running'.")
    return

def aura_api_connect(dot_ini_file = 'neo4jConfig.ini') -> dict :
    # need to provide the file name that contains the values for the AURA_* variable values
    # other options are to check for environment variables to get the Aura credentials.
    AURA_API, AURA_URL, AURA_TOKEN_URL, AURA_API_CLIENT_ID, AURA_CLIENT_SECRET = \
        read_neo4j_properties(dot_ini_file)
    aura_headers = aura_set_request_header(AURA_API_CLIENT_ID, AURA_CLIENT_SECRET, AURA_TOKEN_URL)
    inst_id = aura_inst_id(AURA_URL)
    return aura_inst_info(AURA_API, inst_id, aura_headers)


def aura_print_inst_info(inst_info):
    print(pd.DataFrame([inst_info], columns=['name', 'connection_url', 'status', 'memory', 'info_updated', 'cloud_provider', 'id'],
                       index=['']).transpose())
    return

