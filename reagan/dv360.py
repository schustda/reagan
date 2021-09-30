from googleapiclient.discovery import build
from google.oauth2 import service_account
from reagan.subclass import Subclass
from io import StringIO
import pandas as pd
import os
from time import time, sleep
from retrying import retry


class DV360(Subclass):
    def __init__(self, partner_id, version = 'v1', verbose=0, service_account_alias = None):
        super().__init__(verbose=verbose)
        self.partner_id = partner_id
        self.version = version
        self.service_account_filepath = self.get_parameter_value(f'''/dv360/{"service_account_path" if not service_account_alias else service_account_alias}''')
        self._create_service()
        self.api_calls = 0

    @retry(stop_max_attempt_number=10, wait_fixed=10000)
    def _create_service(self):
        api_name = "displayvideo"
        self.credentials = service_account.Credentials.from_service_account_file(self.service_account_filepath)
        self.service = build(api_name, self.version, credentials=self.credentials)

    def _add_missing(self, response, arguments):
        ids_recieved = set([int(obj['id']) for obj in response])
        ids_requested = set(arguments.get('ids',[]))
        for missing_id in ids_requested - ids_recieved:
            response.append({'id':str(missing_id)})
        return response

    def decode_error(self, error):
        # Returns a more concise error description
        return eval(error.content.decode())['error']['message']

    def list(self, obj, arguments={}, all=False):
        """
        Calls the list method for the DV360 Api.
            - obj (string): The api object to pull from
            - arguments (dict): Any additional arguments to pass to the list method.
                (note: partner_id is automatically added)
            - all (bool): Whether to make continuous api calls or just a single
        """

        request = eval(f'self.service.{obj}().list(**arguments)')
        return request.execute()

    def update(self, obj, body, arguments={}):
        """
        Calls the update method for the DCM Api.
            - obj (string): The api object to pull from
            - body (dict): The object body for which to update to dcm
            - arguments (dict): Any additional arguments to pass. (Not Required)
                (note: partner_id is automatically added)
        """

        arguments["body"] = body
        request = eval("self.service.{0}().update(**arguments)".format(obj))
        self.api_calls += 1
        return request.execute()

    def insert(self, obj, body, arguments={}):
        """
        Calls the insert method for the DCM Api.
            - obj (string): The api object to pull from
            - body (dict): The object body for which to update to dcm
            - arguments (dict): Any additional arguments to pass. (Not Required)
                (note: partner_id is automatically added)
        """

        arguments["body"] = body
        request = eval("self.service.{0}().insert(**arguments)".format(obj))
        self.api_calls += 1
        return request.execute()

    def get(self, obj, id, arguments={}):
        """
        Calls the get method for the DCM Api.
            - obj (string): The api object to pull from
            - body (dict): The object body for which to update to dcm
            - arguments (dict): Any additional arguments to pass. (Not Required)
                (note: partner_id is automatically added)
        """

        arguments["id"] = id
        try:
            request = eval("self.service.{0}().get(**arguments)".format(obj))
            response = request.execute()
            self.api_calls += 1
            return response
        except:
            return {'id':str(id)}

    def patch(self, obj, body, params={}):
        """
        Calls the patch method for the DCM Api.
        - obj (string): The api object to pull from
        - body (dict): The object body for which to update to dcm
        - arguments (dict): Any additional arguments to pass. (Not Required)
        (note: partner_id is automatically added)
        """

        params["body"] = body
        request = eval("self.service.{0}().patch(**params)".format(obj))
        self.api_calls += 1
        return request.execute()

    def to_df(self, obj, arguments={}, columns=None):
        """
        Calls the list method for the DCM Api.
            - obj (string): The api object to pull from
            - arguments (dict): Any additional arguments to pass to the list method.
                (note: partner_id is automatically added)
            - columns (list): If datatype is set to df, reduce the
                dataframe to only return the specified columns
            - all (bool): Whether to make continuous api calls or just a single
            - method (string): Which API endpoint to call. Only supports list and get.
        """
        next_page_token, first_call = None, True
        while next_page_token or first_call:
            first_call = False
            
            response = self.list(obj=obj, arguments=arguments)

            s = set(response.keys()) - set(["kind", "nextPageToken"])
            if s:
                obj_key = (s).pop()
            else:
                break
            data = response[obj_key]
            
            next_page_token = response.get('nextPageToken',None)
            arguments['pageToken'] = next_page_token
            
            yield self._json_to_df(data, columns)

if __name__ == "__main__":
    dv360 = DV360(partner_id = 1982032)
    for df in dv360.to_df('advertisers().creatives',columns = ['creativeId','displayName'],arguments={'advertiserId':'2123140'}):
        print(df.head())
