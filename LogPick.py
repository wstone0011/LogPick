#encoding:utf-8
import os
import queue
import json
import hashlib

class LogPick(object):
    lst_suffix = [".json"]
    file_record = "record.json"
    file_result = "result.json"
    
    def __init__(self, dir, prettyrecord=False):
        self.dir = dir
        self.prettyrecord = prettyrecord
        self.q = queue.Queue()
        self.cache = {}
        
    def writeFile(self, file, msg):
        fout = open(file, "a+", encoding="utf-8")
        fout.write(msg)
        fout.write("\n")
        fout.close()
    
    def sha1(self, msg):
        h = hashlib.sha1()
        h.update(msg)
        return h.hexdigest()
        
    def isOK(self, file):
        bFlag = False
        for _ in self.lst_suffix:
            if file.endswith(_):
                bFlag = True
                break
                
        return bFlag
        
    def eachFile(self, dir, recursive=False, isOK=None):
        lst = []
        if not recursive:
            for _ in os.listdir(dir):
                file = dir+"/"+_
                if os.path.isfile(file):
                    if not isOK:
                        lst.append(file)
                    else:
                        if isOK(file):
                            lst.append(file)
        else:
            self.q.put(dir)
            while not self.q.empty():
                dir = self.q.get()
                for _ in os.listdir(dir):
                    file = dir+"/"+_
                    if os.path.isfile(file):
                        if not isOK:
                            lst.append(file)
                        else:
                            if isOK(file):
                                lst.append(file)
                    elif os.path.isdir(file):
                        self.q.put(file)
            
        return lst
        
    def dictWalk(self, dic):
        lst = []
        q = queue.Queue()
        q.put(["", dic])
        while not q.empty():
            [prefix, dic] = q.get()
            for _ in dic:
                bFlagPut = False
                if isinstance(dic[_], dict):
                    q.put([prefix+"."+_, dic[_]])
                    bFlagPut = True
                elif isinstance(dic[_], list):
                    if dic[_]:
                        if isinstance(dic[_][0], dict):
                            q.put([prefix+"."+_+"|", dic[_][0]])
                            bFlagPut = True
                            
                if not bFlagPut:
                    lst.append(prefix+"."+_)
        lst.sort()
        
        s = "&&".join(lst)
        # return self.sha1(s.encode())
        return s
        
    def pick(self, log):
        bFlag = False
        try:
            struct_id = self.dictWalk(log)
            if struct_id not in self.cache:
                self.cache[struct_id] = log  # pick it
                bFlag = True
            
        except Exception as e:
            print(e)
        
        return bFlag
        
    
    def run(self):
        print("[+] start")
        lst = self.eachFile(self.dir, recursive=True, isOK=self.isOK)
        for _ in lst:
            try:
                print("[+] file: "+_)
                fin = open(_, encoding="utf-8", errors="replace")  #!!
                while 1:
                    try:
                        line = fin.readline()
                        if not line:
                            break
                        print(".", end="")
                        log = json.loads(line, encoding="utf-8")
                        bFlag = self.pick(log)
                        if bFlag:
                            self.writeFile(self.file_record, json.dumps(log))
                    except Exception as e:
                        print(e)
                        
                fin.close()
                print("")
                
            except Exception as e:
                print(e)
        
        if self.cache:
            lst = [x for x in self.cache]
            lst.sort()
            for i,_ in enumerate(lst):
                log = self.cache[_]
                if self.prettyrecord:
                    msg  = "%u. \n"%(i+1)
                    msg += json.dumps(log, ensure_ascii=False, indent=4, sort_keys=True)
                    msg += "\n"
                    self.writeFile(self.file_result, msg)
                else:
                    self.writeFile(self.file_result, json.dumps(log, ensure_ascii=False))
                
        print("")
        print("[+] game over.")
        
def main():
    m = LogPick("logs", prettyrecord=True)
    m.run()
    
if "__main__"==__name__:
    main()
    