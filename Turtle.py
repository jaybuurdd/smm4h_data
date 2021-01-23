import os
import re
import pathlib
import pickle
import pathlib

class Job:
    def __init__(self, progress_file_name: str):
        self.path = os.getcwd() + os.path.sep + progress_file_name
        self.dir = os.path.sep.join(self.path.split(os.path.sep)[0:-1])
        pathlib.Path(self.dir).mkdir(parents=True, exist_ok=True)
        self.processed = set()
        if pathlib.Path(self.path).exists():
            with open(self.path, "rt") as f:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    self.processed.add(line.strip())
        self.progress_file = open(self.path, "at")

    def add_done(self, item: str):
        if not self.is_done(item):
            self.processed.add(item)
            self.progress_file.write(item + os.linesep)

    def is_done(self, item: str):
        return item in self.processed

    def close(self):
        self.progress_file.close()

class ResultStore:
    def __init__(self, root:str, num_pre: int, size_seg: int, dflt='_'):
        self.root = root
        self.size_seg = size_seg
        self.num_pre = num_pre
        self.size_pre = num_pre * size_seg
        self.filler = dflt * self.size_pre
    def clean_chars(self, name:str):
        return re.sub("[^A-z0-9]","_",name)
    def chop_pre(self, pre:str):
        return os.path.sep.join(list(
            map(lambda x: self.clean_chars(pre)[x*self.size_seg:x*self.size_seg+self.size_seg], 
                range(0,self.num_pre))))
    def mkdir(self, path):
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    def mk_path(self, fname:str):
        pre_path = self.chop_pre((fname + self.filler)[0:self.size_pre])
        return self.root + os.path.sep + pre_path + os.path.sep + fname
    def dump(self, filename:str, obj):
        path = self.mk_path(filename)
        dir_path= os.path.sep.join(path[::-1].split(os.path.sep)[1:])[::-1]
        self.mkdir(dir_path)
        with open(self.mk_path(filename), 'wb') as output:
            pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)
    def load(self, filename:str):
        with open(self.mk_path(filename), "rb") as f:
           return pickle.load(f)
