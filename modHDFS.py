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

    def listDir(self, dirname:str='/'):
        return self.connect.list(dirname)

    def getFiles(self, dirname:str, depth:int=0) -> list:
        l = []
        if not dirname:
            print("dirname is null")
        else:
            for file in self.connect.walk(dirname, depth = depth):
                if file[-1]:
                    for f in file[-1]:
                        l.append(file[0]+'/'+f)
            return l

    def downloadToCsv(self, filename:str) -> None:
        '''only split for the '€€' sign, and generate same filename in current directory'''
        with self.connect.read(filename, encoding='utf-8') as reader:
            with open(csvdir+filename.split('/')[-1].split('.')[0]+'.csv', 'a+') as cf:
                for line in reader.readlines():
                    newline = line.replace('€€', ',')
                    cf.write(newline)

class uploadHDFS(interHDFS):
    '''default directory is "/home"'''
    def __init__(self, url, csvdir='/home', user=None, **kwargs):
        super().__init__(url, user, csvdir='home')
        self.csvdir = csvdir

    def upload(self, hdfs_path):
        try:
            self.connect.upload(hdfs_path, self.csvdir)
        except Exception as e:
            print(f"[ERROR]:{e}")

