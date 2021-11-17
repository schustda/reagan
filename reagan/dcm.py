from googleapiclient.discovery import build
from google.oauth2 import service_account
from reagan.subclass import Subclass
from io import StringIO
import pandas as pd
import os
from time import time, sleep
from retrying import retry


class DCMAPI(Subclass):
    def __init__(self, networkId=8334, version = None, verbose=0, profile_id=None, service_account_alias = None):
        super().__init__(verbose=verbose)
        self.version = version
        self.service_account_filepath = self.get_parameter_value(f'''/dcm/{"service_account_path" if not service_account_alias else service_account_alias}''')
        self._create_service()
        self.dcm_api_calls = 0
        self.profile_id = profile_id or self.set_profile_id(networkId)
        self.version = version or self.get_parameter_value('/dcm/api_version')

    @retry(stop_max_attempt_number=10, wait_fixed=10000)
    def _create_service(self):

        api_name = "dfareporting"
        self.credentials = service_account.Credentials.from_service_account_file(
            self.service_account_filepath
        )
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
        Calls the list method for the DCM Api.
            - obj (string): The api object to pull from
            - arguments (dict): Any additional arguments to pass to the list method.
                (note: profile_id is automatically added)
            - all (bool): Whether to make continuous api calls or just a single
        """

        arguments["profileId"] = self.profile_id
        if obj == 'placementTags':
            request = eval("self.service.placements().generatetags(**arguments)")
        else:
            request = eval("self.service.{0}().list(**arguments)".format(obj))
        self.dcm_api_calls += 1

        output = []

        while True:
            response = request.execute()
            self.dcm_api_calls += 1

            # Need to re-work this logic below on how to get the key with the data (it's different then than the api object)
            s = set(response.keys()) - set(["kind", "nextPageToken"])
            if s:
                obj_key = (s).pop()
            else:
                break
            data = response[obj_key]
            output.extend(data)

            if not all:
                break
            elif response[obj_key] and response.get("nextPageToken",0) and len(data) == 1000:
                request = eval("self.service.{0}().list_next(request, response)".format(obj))
            else:
                break
        if 'ids' in arguments:
            output = self._add_missing(output, arguments)

        self.vprint(f"Complete. Made {self.dcm_api_calls} API call(s).")
        return output

    def update(self, obj, body, arguments={}):
        """
        Calls the update method for the DCM Api.
            - obj (string): The api object to pull from
            - body (dict): The object body for which to update to dcm
            - arguments (dict): Any additional arguments to pass. (Not Required)
                (note: profile_id is automatically added)
        """

        arguments["profileId"] = self.profile_id
        arguments["body"] = body
        request = eval("self.service.{0}().update(**arguments)".format(obj))
        self.dcm_api_calls += 1
        return request.execute()

    def insert(self, obj, body, arguments={}):
        """
        Calls the insert method for the DCM Api.
            - obj (string): The api object to pull from
            - body (dict): The object body for which to update to dcm
            - arguments (dict): Any additional arguments to pass. (Not Required)
                (note: profile_id is automatically added)
        """

        arguments["profileId"] = self.profile_id
        arguments["body"] = body
        request = eval("self.service.{0}().insert(**arguments)".format(obj))
        self.dcm_api_calls += 1
        return request.execute()

    def get(self, obj, id, arguments={}):
        """
        Calls the get method for the DCM Api.
            - obj (string): The api object to pull from
            - body (dict): The object body for which to update to dcm
            - arguments (dict): Any additional arguments to pass. (Not Required)
                (note: profile_id is automatically added)
        """

        arguments["profileId"] = self.profile_id
        arguments["id"] = id
        try:
            request = eval("self.service.{0}().get(**arguments)".format(obj))
            response = request.execute()
            self.dcm_api_calls += 1
            return response
        except:
            return {'id':str(id)}

    def patch(self, obj, body, params={}):
        """
        Calls the patch method for the DCM Api.
        - obj (string): The api object to pull from
        - body (dict): The object body for which to update to dcm
        - arguments (dict): Any additional arguments to pass. (Not Required)
        (note: profile_id is automatically added)
        """

        params["profileId"] = self.profile_id
        params["body"] = body
        request = eval("self.service.{0}().patch(**params)".format(obj))
        self.dcm_api_calls += 1
        return request.execute()

    def to_df(self, obj, arguments={}, columns=None, all=False, dropna=False, method='list'):
        """
        Calls the list method for the DCM Api.
            - obj (string): The api object to pull from
            - arguments (dict): Any additional arguments to pass to the list method.
                (note: profile_id is automatically added)
            - columns (list): If datatype is set to df, reduce the
                dataframe to only return the specified columns
            - all (bool): Whether to make continuous api calls or just a single
            - dropna (bool): Whether or not the dataframe can contain nulls
            - method (string): Which API endpoint to call. Only supports list and get.
        """
        if method == 'list':
            data = self.list(obj=obj, arguments=arguments, all=all)
        elif method == 'get':
            obj_id = arguments.pop('id')
            data = [self.get(obj=obj, id=obj_id, arguments=arguments)]
        df = self._json_to_df(data, columns)
        return df

    def report_to_df(self, reportId, fileId = None, params={}):
        """
        Makes a GET request to retrieve a file and returns the data in a Pandas Dataframe
            - reportId (string): The id of the report to pull from
            - fildId (string): The id of the file to pull from
            - params (dict): Any additional arguments to pass to the GET request.
        """

        skiprows = 5
        if not fileId:
            skiprows = 4
            report_file = (
                self.service.reports()
                .run(profileId=self.profile_id, reportId=reportId)
                .execute()
            )
            fileId = report_file["id"]
            # Wait for the report file to finish processing.
            # An exponential backoff strategy is used to conserve request quota.
            while True:
                report_file = (
                    self.service.files().get(reportId=reportId, fileId=fileId).execute()
                )

                status = report_file["status"]
                if status == "REPORT_AVAILABLE":
                    break
                sleep(60)

        params["reportId"] = reportId
        params["fileId"] = fileId

        request = self.service.files().get_media(**params)
        response = request.execute()

        s = str(response, "utf-8")
        data = StringIO(s)

        report_metadata = pd.read_csv(data, error_bad_lines=False, warn_bad_lines=False)
        
        skiprows = report_metadata.shape[0] + 4

        s = str(response, "utf-8")
        data = StringIO(s)
        df = pd.read_csv(data, skiprows=skiprows)
        # Remove the last row with the grand total
        return df[:-1]

    def set_profile_id(self, networkId):
        request = self.service.userProfiles().list()

        # Execute request and print response.
        response = request.execute()
        for profile in response["items"]:
            if profile["accountId"] == str(networkId):
                self.profile_id = profile["profileId"]

            self.vprint(
                f'Found user profile with ID {profile["profileId"]} and user name {profile["userName"]}.'
            )


if __name__ == "__main__":

    dcm = DCMAPI(networkId = 8334, verbose=1,service_account_alias='service_account_path2')
    columns = ['id','name','accountId','advertiserId','campaignId']
    obj='placements'
    ids = [252947455
        ,259790714,2956747
    ]
    arguments = {'ids':ids}
    z = dcm.to_df(obj=obj,arguments=arguments, columns = columns)
    # arguments = {'id':2956747}
    # df1 = dcm.to_df(obj=obj,columns = columns,arguments=arguments,method='get')
    # arguments = {'id':3339876}
    # df2 = dcm.to_df(obj=obj,columns = columns,arguments=arguments,method='get')