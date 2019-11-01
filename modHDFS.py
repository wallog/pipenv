from hdfs import InsecureClient

'''Currently supports reading TXT files from HDFS, replace characters, and convert to CSV files,
   You can also specify the project ID and parse the file directly to elemental'''

class interHDFS:
    def __init__(self, url, user=None, **kwargs):
        self.url = url
        self.user = user
        for k, v in kwargs.items():
            self.k = v
        self.connect = InsecureClient(self.url, self.user)
        try:
            self.connect.status('/')
        except Exception as e:
            print(f"[ERROR]:")
            raise("connected failed!")

    @property
    def apiVersion(self):
        return "v1"

    def listdir(self, dirname:str='/'):
        return self.connect.list(dirname)

    def getfiles(self, dirname:str, depth:int=0) -> list:
        l = []
        if not dirname:
            print("dirname is null")
        else:
            for file in self.connect.walk(dirname, depth = depth):
                if file[-1]:
                    for f in file[-1]:
                        l.append(file[0]+'/'+f)
            return l

    def gencsv(self, filename:str):
        '''only split for the '€€' sign, and generate same filename in current directory'''
        with self.connect.read(filename, encoding='utf-8') as reader:
            with open(filename.split('/')[-1].split('.')[0]+'.csv', 'w+') as cf:
                for line in reader.readlines():
                    newline = line.replace('€€', ',')
                    cf.write(newline)


