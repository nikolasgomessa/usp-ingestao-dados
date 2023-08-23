import json
    
class JSONLoader:
    @staticmethod
    def load_json(file_path, encoding='utf-8'):
        with open(file_path, 'r', encoding=encoding) as json_file:
            data = json.load(json_file)
        return data
