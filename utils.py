import xxhash
import os
import json

count = 0
def countFiles(dir):
    global count
    count = 0
    def core(dir):
        global count
        for f in os.listdir(dir):
            fname = os.path.join(dir,f)
            if os.path.isfile(fname):
                count += 1
            if os.path.isdir(fname):
                core(fname)
    core(dir)
    return count

def countDBFiles(node):
    global count
    count = 0
    def core(node):
        global count
        count += len(node["files"])
        for d in node["directories"]:
            core(d)
    core(node)
    return count

def hashFile(file_path):
    hasher = xxhash.xxh64()

    with open(file_path, 'rb') as file:
        while True:
            data = file.read(4096)
            if not data:
                break
            hasher.update(data)
    return hasher.hexdigest()

def loadDB(dirname:str) -> dict:
    dbFname = os.path.join(dirname,"fsync.db")
    if not os.path.isfile( dbFname ): 
        raise Exception(f"No database found in {dirname}")
    return json.loads(open(dbFname,"r").read())

def saveDB(dirname:str,db:dict):
    dbFname = os.path.join(dirname,"fsync.db")
    if not os.path.isdir(dirname):
        raise Exception(f"Invalid directory {dirname}")
    open(dbFname,"w").write(json.dumps(db,indent=3))