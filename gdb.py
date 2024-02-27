import requests
import base64
import json

class gdb:
    def __init__(self, dbpath, token):
        self.dbpath = dbpath
        self.auth = ("gdb/1.1", token)
        self.content_dict, self.sha = self._get_content()

    def _get_content(self):
        response = requests.get(self.dbpath, auth=self.auth)
        if response.status_code == 200:
            content = base64.b64decode(response.json()["content"]).decode("utf-8")
            return json.loads(content), response.json().get("sha")
        else:
            raise Exception("Unable to get content")

    def _put_content(self, content_dict, sha, message="update"):
        content = base64.b64encode(json.dumps(content_dict).encode("utf-8")).decode("utf-8")
        data = {
            "message": message,
            "content": content,
            "sha": sha
        }
        response = requests.put(self.dbpath, auth=self.auth, json=data)
        if response.status_code != 200:
            raise Exception("Unable to put content")

    def add(self, data):
        self.content_dict, self.sha = self._get_content()
        for key in self.content_dict["config"]["keys"]:
            if key not in data:
                data[key] = self.content_dict["config"]["keys"][key]
        for key in data:
            if key not in self.content_dict["config"]["keys"]:
                raise Exception(f"Key {key} is not in config")
        id_counter = self.content_dict["config"]["id_counter"]
        self.content_dict["data"][str(id_counter)] = data
        self.content_dict["config"]["id_counter"] += 1
        self._put_content(self.content_dict, self.sha)

    def clear(self):
        self.content_dict, self.sha = self._get_content()
        self.content_dict["data"] = {}
        self._put_content(self.content_dict, self.sha)

    def config(self):
        self.content_dict, self.sha = self._get_content()
        return self.content_dict["config"]

    def data(self):
        self.content_dict, self.sha = self._get_content()
        return self.content_dict["data"]

    def find(self, filter):
        self.content_dict, self.sha = self._get_content()
        if isinstance(filter, int):
            if str(filter) not in self.content_dict["data"]:
                raise KeyError(f"id {filter} not found")
            return {str(filter): self.content_dict["data"][str(filter)]}
        elif isinstance(filter, list):
            result = {}
            for id in filter:
                if str(id) not in self.content_dict["data"]:
                    raise KeyError(f"id {id} not found")
                result[str(id)] = self.content_dict["data"][str(id)]
            return result
        elif isinstance(filter, dict):
            result = {}
            for id, data in self.content_dict["data"].items():
                if all(data.get(key) == value for key, value in filter.items()):
                    result[id] = data
            return result
        else:
            raise TypeError("filter must be an int, a list or a dict")

    def keys(self):
        self.content_dict, self.sha = self._get_content()
        return self.content_dict["config"]["keys"]

    def remove(self, filter):
        self.content_dict, self.sha = self._get_content()
        if isinstance(filter, int):
            if str(filter) not in self.content_dict["data"]:
                raise KeyError(f"id {filter} not found")
            del self.content_dict["data"][str(filter)]
        elif isinstance(filter, list):
            for id in filter:
                if str(id) not in self.content_dict["data"]:
                    raise KeyError(f"id {id} not found")
                del self.content_dict["data"][str(id)]
        elif isinstance(filter, dict):
            for id, data in list(self.content_dict["data"].items()):
                if all(data.get(key) == value for key, value in filter.items()):
                    del self.content_dict["data"][id]
        else:
            raise TypeError("filter must be an int, a list or a dict")
        self._put_content(self.content_dict, self.sha)

    def reset(self):
        self.content_dict, self.sha = self._get_content()
        self.content_dict["data"] = {}
        self.content_dict["config"]["id_counter"] = 1
        self._put_content(self.content_dict, self.sha)

    def update(self, filter, new_data):
        self.content_dict, self.sha = self._get_content()
        if isinstance(filter, int):
            if str(filter) not in self.content_dict["data"]:
                raise KeyError(f"id {filter} not found")
            self.content_dict["data"][str(filter)].update(new_data)
        elif isinstance(filter, list):
            for id in filter:
                if str(id) not in self.content_dict["data"]:
                    raise KeyError(f"id {id} not found")
                self.content_dict["data"][str(id)].update(new_data)
        elif isinstance(filter, dict):
            for id, data in self.content_dict["data"].items():
                if all(data.get(key) == value for key, value in filter.items()):
                    data.update(new_data)
        else:
            raise TypeError("filter must be an int, a list or a dict")
        self._put_content(self.content_dict, self.sha)
