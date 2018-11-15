# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright (c) 2018-, Timothée Chabat.
# Copyright (c) 2018-, Daniel Althviz.
# Object-oriented CARMIN API Client
# -----------------------------------------------------------------------------

import requests
import os.path as osp

# ================================= CONSTANTS =================================
# -----------------------------------------------------------------------------
API_URL = "https://vip.creatis.insa-lyon.fr/rest/"
API_KEY = "TheApiKey"


class PyCarmin(object):
    """
    Carmin API client to use with VIP.
    """

    def __init__(self, apiKey=API_KEY, apiUrl=API_URL,
                 cert_path=osp.join(osp.dirname(__file__), 'certif.crt')):
        self.__PREFIX = apiUrl
        self.__apikey = apiKey
        self.__headers = {'apikey': self.__apikey}

        if not osp.exists(cert_path):
            self.__certif = None
        else:
            self.__certif = cert_path

    # -------------------------------------------------------------------------
    def getApiKey(self):
        """
        Get the current api key.
        """
        return self.__apikey

    def getApiUrl(self):
        """
        Get the current api url.
        """
        return self.__PREFIX

    def setApiKey(self, value):
        """
        Return True is correct apikey, False otherwise.
        Raise an error if an other problems occured
        """
        url = self.__PREFIX + 'plateform'
        head_test = {
                     'apikey': value,
                    }
        rq = requests.put(url, headers=head_test, verify=self.__certif)
        res = self.detect_errors(rq)
        if res[0]:
            if res[1] == 40101:
                return False
            else:
                raise RuntimeError("Error {} from VIP : {}".format(res[1],
                                                                   res[2]))
        else:
            self.__apikey = value
            self.__headers['apikey'] = self.__apikey
            return True

    # -------------------------------------------------------------------------
    def setCertifPath(self, path):
        """
        TODO : verify if the certif work
        """
        self.__certif = path
        return osp.isfile(self.__certif)

    # -------------------------------------------------------------------------
    def detect_errors(self, req):
        """
        [0]True if an error, [0]False otherwise
        If True, [1] and [2] are error details.
        """
        try:
            res = req.json()
        except Exception:
            return (False,)
        else:
            if isinstance(res, dict) and \
               list(res.keys()) == ['errorCode', 'errorMessage']:
                return (True, res['errorCode'], res['errorMessage'])

        return (False,)

    # -------------------------------------------------------------------------
    def manage_errors(self, req):
        """
        raise an runtime error if the result of a request is an error message
        """
        res = self.detect_errors(req)
        if res[0]:
            raise RuntimeError("Error {} from VIP : {}".format(res[1], res[2]))

    # ================================= PATH ==================================
    # -------------------------------------------------------------------------
    def create_dir(self, path):
        """
        Return True if done, False otherwise
        """
        url = self.__PREFIX + 'path' + path
        rq = requests.put(url, headers=self.__headers, verify=self.__certif)
        try:
            self.manage_errors(rq)
        except RuntimeError:
            return False
        else:
            return True

    # -------------------------------------------------------------------------
    def create_dir_smart(self, path):
        """
        If 'path' already exist, add a number suffix

        'path' should NOT have a '/' at the end
        return a path with the same syntax
        """
        ind = 0
        res_path = path
        while self.exists(res_path):
            ind += 1
            res_path = path + str(ind)

        self.create_dir(res_path)
        return res_path

    # -----------------------------------------------------------------------------
    def _path_action(self, path, action):
        """
        Be carefull tho because 'md5' seems to not work.
        Also 'content' is not accepted here, use download() function instead.
        """
        assert action in ['list', 'exists', 'properties', 'md5']
        url = self.__PREFIX + 'path' + path + '?action=' + action
        rq = requests.get(url, headers=self.__headers, verify=self.__certif)
        self.manage_errors(rq)
        return rq

    # -------------------------------------------------------------------------
    def list_content(self, path):
        return self._path_action(path, 'list').json()

    # -------------------------------------------------------------------------
    def list_directory(self, path):
        res = self.list_content(path)
        return [d for d in res if d['isDirectory']]

    # -------------------------------------------------------------------------
    def list_elements(self, path):
        res = self.list_content(path)
        return [e for e in res if not e['isDirectory']]

    # -----------------------------------------------------------------------------
    def exists(self, path):
        return self._path_action(path, 'exists').json()['exists']

    # -----------------------------------------------------------------------------
    def get_path_properties(self, path):
        return self._path_action(path, 'properties').json()

    # -----------------------------------------------------------------------------
    def is_dir(self, path):
        return self.get_path_properties(path)['isDirectory']

    # -----------------------------------------------------------------------------
    def delete_path(self, path):
        """
        Delete a file or a path (with all its content).
        Return True if done, False otherwise
        """
        url = self.__PREFIX + 'path' + path
        rq = requests.delete(url, headers=self.__headers, verify=self.__certif)
        try:
            self.manage_errors(rq)
        except RuntimeError:
            return False
        else:
            return True

    # -----------------------------------------------------------------------------
    def upload(self, path, where_to_save):
        """
        Input:
            - path : on local computer, the file to upload
            - where_to_save : on VIP, something like "/vip/Home/RandomName.ext"

        Return True if done, False otherwise
        """
        url = self.__PREFIX + 'path' + where_to_save
        headers = {
                    'apikey': self.__apikey,
                    'Content-Type': 'application/octet-stream',
                  }
        data = open(path, 'rb').read()
        rq = requests.put(url, headers=headers, data=data,
                          verify=self.__certif)
        try:
            self.manage_errors(rq)
        except RuntimeError:
            return False
        else:
            return True

    # -------------------------------------------------------------------------
    def download(self, path, where_to_save):
        """
        Input:
            - path: on VIP, something like "/vip/Home/RandomName.ext",
                    content to dl
            - where_to_save : on local computer
        """
        url = self.__PREFIX + 'path' + path + '?action=content'
        rq = requests.get(url, headers=self.__headers, stream=True,
                          verify=self.__certif)
        try:
            self.manage_errors(rq)
        except RuntimeError:
            return False
        else:
            with open(where_to_save, 'wb') as out_file:
                out_file.write(rq.content)
            return True

    # ============================== EXECUTIONS ===============================
    # -------------------------------------------------------------------------
    def list_executions(self):
        url = self.__PREFIX + 'executions'
        rq = requests.get(url, headers=self.__headers, verify=self.__certif)
        self.manage_errors(rq)
        return rq.json()

    # -------------------------------------------------------------------------
    def count_executions(self):
        url = self.__PREFIX + 'executions/count'
        rq = requests.get(url, headers=self.__headers, verify=self.__certif)
        self.manage_errors(rq)
        return int(rq.text)

    # -------------------------------------------------------------------------
    def init_exec(self, pipeline, name="default", inputValues={}):
        url = self.__PREFIX + 'executions'
        headers = {
                    'apikey': self.__apikey,
                    'Content-Type': 'application/json'
                  }
        data_ = {
                "name": name,
                'pipelineIdentifier': pipeline,
                "inputValues": inputValues
               }
        rq = requests.post(url, headers=headers, json=data_,
                           verify=self.__certif)
        self.manage_errors(rq)
        return rq.json()["identifier"]

    # -------------------------------------------------------------------------
    def execution_info(self, id_exec):
        url = self.__PREFIX + 'executions/' + id_exec
        rq = requests.get(url, headers=self.__headers, verify=self.__certif)
        self.manage_errors(rq)
        return rq.json()

    # -------------------------------------------------------------------------
    def is_running(self, id_exec):
        info = self.execution_info(id_exec)
        return info['status'] == 'Running'

    # -------------------------------------------------------------------------
    def get_exec_stderr(self, exec_id):
        url = self.__PREFIX + 'executions/' + exec_id + '/stderr'
        rq = requests.get(url, headers=self.__headers, verify=self.__certif)
        self.manage_errors(rq)
        return rq.text

    # -------------------------------------------------------------------------
    def get_exec_stdout(self, exec_id):
        url = self.__PREFIX + 'executions/' + exec_id + '/stdout'
        rq = requests.get(url, headers=self.__headers, verify=self.__certif)
        self.manage_errors(rq)
        return rq.text

    # -------------------------------------------------------------------------
    def get_exec_results(self, exec_id):
        url = self.__PREFIX + 'executions/' + exec_id + '/results'
        rq = requests.get(url, headers=self.__headers, verify=self.__certif)
        self.manage_errors(rq)
        return rq.json()

    # -------------------------------------------------------------------------
    def kill_execution(self, exec_id, deleteFiles=False):
        url = self.__PREFIX + 'executions/' + exec_id
        if deleteFiles:
            url += '?deleteFiles=true'
        rq = requests.delete(url, headers=self.__headers, verify=self.__certif)
        try:
            self.manage_errors(rq)
        except RuntimeError:
            return False
        else:
            return True

    # ============================== PIPELINES ================================
    # -------------------------------------------------------------------------
    def list_pipeline(self):
        url = self.__PREFIX + 'pipelines'
        rq = requests.get(url, headers=self.__headers, verify=self.__certif)
        self.manage_errors(rq)
        return rq.json()

    # -------------------------------------------------------------------------
    def pipeline_def(self, pip_id):
        url = self.__PREFIX + 'pipelines/' + pip_id
        rq = requests.get(url, headers=self.__headers, verify=self.__certif)
        self.manage_errors(rq)
        return rq.json()

    # ================================ OTHER ==================================
    # -------------------------------------------------------------------------
    def platform_info(self):
        url = self.__PREFIX + 'platform/'
        rq = requests.get(url, headers=self.__headers, verify=self.__certif)
        self.manage_errors(rq)
        return rq.json()

    # -------------------------------------------------------------------------
    def get_apikey(self, username, password):
        """
        username is the email account you used to create your VIP account
        """
        url = self.__PREFIX + 'authenticate'
        headers = {
                    'apikey': self.__apikey,
                    'Content-Type': 'application/json'
                  }
        data_ = {
                "username": username,
                "password": password
               }
        rq = requests.post(url, headers=headers, json=data_,
                           verify=self.__certif)
        self.manage_errors(rq)
        return rq.json()['httpHeaderValue']


# =============================================================================
if __name__ == '__main__':
    carminClient = PyCarmin()
    print carminClient
    print carminClient.getApiKey()
    print carminClient.getApiUrl()
    print carminClient.list_pipeline()
