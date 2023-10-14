import os,shutil                     # filesystem management
import utils                         # DB management functions
import sys                           # input arguments
from alive_progress import alive_bar # loading bar
import re                            # regex for ignored files
import argparse                      # to parse input arguments
# Database structure:
# {
#   "name":"root"
# 	"files":[
#       {
#          "name":hello_world.txt
#          "lastedit":1920398122344
#          "size":size in bytes of the file
#          "hash":xxhash of the file
#       },
#       ...
# 	]
#   "directories":[
#       {
#          "name":name of the directory
#          "files":[...],
#          "directories":[...]
#       },
#       ...
#   ]
# }
# The database is saved as a json encoded string to a file.
# A layer of compression could be added to this stack.

pj = os.path.join
rp = os.path.relpath

excludePatterns = []

# srcBaseDir: root directory src come argomento di fsync
# srcDir: directory figlia di srcBaseDir (o al più coincidente con srcBaseDir) da copiare
# dstDir: directory destinazione in cui copiare srcBaseDir. Non deve esistere
def copyTreeWithIgnores(srcBaseDir,srcDir,dstDir):
    # Funzione ricorsiva che butta fuori una lista di file da copiare, filtrata con gli ignore
    def getDirList(srcBaseDir,srcDir,currDir=None,dirList=[]):
        if currDir is None: currDir = srcDir
        for f in os.listdir(currDir):
            fname = pj(currDir,f)
            if isIgnored( rp( fname,srcBaseDir ) ): continue
            dirList.append( rp(fname,srcDir) )
            if os.path.isdir( fname ):
                getDirList(srcBaseDir,srcDir,currDir=fname,dirList=dirList)
        return dirList

    dirList = getDirList( srcBaseDir, srcDir )
    if os.path.exists( dstDir ):
        raise Exception("dest already exist")
    os.mkdir(dstDir)
    for f in dirList:
        if os.path.isdir(pj(srcDir,f)): os.mkdir(pj(dstDir,f))
        else: shutil.copyfile( pj(srcDir,f), pj(dstDir,f) )

def parseExcludeFile(filename):
    global excludePatterns
    with open(filename) as fin:
        for line in fin.readlines():
            line = line.replace("\r","").replace("\n","")
            if "#" in line: line = line[:line.find("#")]
            if len(line) > 0:
                while len(line) > 0 and (line[0] == " " or line[0] == "\t"): line = line[1:]
                while len(line) > 0 and (line[-1] == " " or line[-1] == "\t"): line = line[:-1]
                excludePatterns.append(line)
                print(f"loaded ep: '{line}'")
            

def isIgnored(filename):
    global excludePatterns
    for pattern in excludePatterns:
        if re.search(pattern=pattern,string=filename) is not None:
            return True
    return False

"""
If dirname already has a database, update it, otherwise, create it.
The database holds information about the size, hash, name, date of last edit
for every file in the directory.
"""
def updateDatabase(dirname,totCount):
    def core(root, dirname, bar, isRoot=True, baseDir=None):
        # Add or update records of existing files if not currently ignored
        for f in os.listdir(dirname):
            fname = pj(dirname, f)
            if not os.path.isfile(fname): continue 
            if isRoot and f == "fsync.db": continue
            if isRoot: baseDir = dirname
            if isIgnored( rp(fname,baseDir) ): 
                continue
            # Update data of this file if it is not already in the DB, or if the last edit date
            # or the size in bytes are different from the recorded ones
            # The main thing here is the hash calculation
            fstat = os.stat(fname)
            file = [a for a in root["files"] if a["name"] == f]
            fileInDB = len(file) > 0
            file = file[0] if fileInDB else None
            if fileInDB and (file["size"] != fstat.st_size or file["lastedit"] != fstat.st_mtime):
                file["lastedit"] = fstat.st_mtime
                file["size"] = fstat.st_size
                file["hash"] = utils.hashFile(fname)
            if not fileInDB:
                file = {"name": f, "lastedit": fstat.st_mtime,
                        "size": fstat.st_size, "hash": utils.hashFile(fname)}
                root["files"].append(file)
            bar()

        # Delete records of files no longer existing or currently ignored
        toRemove = []
        for i in range(len(root["files"])):
            f = root["files"][i]
            fname = pj(dirname, f["name"])
            if not os.path.isfile(fname) or isIgnored( rp(fname,baseDir) ):
                toRemove.append(i-len(toRemove))
        for i in toRemove:
            del root["files"][i]
        toRemove.clear()

        # Delete the records of directories no longer existing or currently ignored
        for i in range(len(root["directories"])):
            d = root["directories"][i]
            dirName = pj(dirname, d["name"])
            if not os.path.isdir(dirName) or isIgnored( rp(dirName,baseDir) ):
                toRemove.append(i-len(toRemove))
        for i in toRemove:
            del root["directories"][i]

        # Adds records of directories not present in DB if they are not ignored and recursively 
        # calls back this function in the subdirectories of root
        for f in os.listdir(dirname):
            xxx = pj(dirname, f)
            if not os.path.isdir(xxx) or isIgnored( rp(xxx,baseDir) ):
                continue
            if f not in [d["name"] for d in root["directories"]]:
                newRoot = {
                    "name": f,
                    "files": [],
                    "directories": []
                }
                root["directories"].append(newRoot)
            else:
                newRoot = [d for d in root["directories"] if d["name"] == f][0]
            core(newRoot, pj(dirname, f), bar, False, baseDir)

    with alive_bar(totCount,spinner="arrows") as bar:
        if not os.path.isdir(dirname):
            raise Exception(f"{dirname} is not a valid directory!")
        if os.path.isfile(pj(dirname, "fsync.db")):
            database = utils.loadDB(dirname)
        else:
            database = {
                "name": "root",
                "files": [],
                "directories": []
            }
        # Update the database with the current filesystem information
        core(database, dirname, bar)

        # Save the database to file
        utils.saveDB(dirname,database)

"""
Synchronizes the two directories, only updating files that are different in size or hash.
Files/directories in dest that are not in src are deleted.
Prints a log of the operations carried out to std::out.
When dryRun is True this function has no effects on the directories, but the log is still printed.
"""
def syncDirectories(srcDirname, dstDirname, dryRun=False):
    if not os.path.isdir(srcDirname):
        raise Exception(f"{srcDirname} is not a valid directory")
    if not os.path.isdir(dstDirname):
        raise Exception(f"{dstDirname} is not a valid directory")
    
    # estimate file count to have a loading bar
    if os.path.isfile( pj(srcDirname,"fsync.db") ):
        srcDB = utils.loadDB(srcDirname)
        srcTotCount = utils.countDBFiles(srcDB)
    else:
        srcTotCount = utils.countFiles(srcDirname)
    if os.path.isfile( pj(dstDirname,"fsync.db") ):
        dstDB = utils.loadDB(dstDirname)
        dstTotCount = utils.countDBFiles(dstDB)
    else:
        dstTotCount = utils.countFiles(dstDirname)

    print("Updating src database...")
    updateDatabase(srcDirname,srcTotCount)
    print("Updating dst database...")
    updateDatabase(dstDirname,dstTotCount)

    srcDB = utils.loadDB(srcDirname)
    dstDB = utils.loadDB(dstDirname)
    
    # Recursive function that synchronizes the inner directories
    def core(srcNode, dstNode, srcDirname, dstDirname, dryRun, bar, dstRoot=None, srcRoot=None):
        if dstRoot is None: dstRoot = dstDirname
        if srcRoot is None: srcRoot = srcDirname
        # Update or add files from srcNode to dstNode
        for sf in srcNode["files"]:
            df = [f for f in dstNode["files"] if f["name"] == sf["name"]]
            df = df[0] if (fileInDst := len(df) > 0) else None
            sFname = pj(srcDirname,sf["name"])
            dFname = pj(dstDirname,sf["name"])
            if not fileInDst:
                print(f"- Adding file <dst>/{rp(dFname,dstRoot)}")
                if not dryRun: shutil.copyfile(sFname,dFname)
            else:
                if sf["size"] != df["size"] or sf["hash"] != df["hash"]:
                    print(f"- Updating file <dst>/{rp(dFname,dstRoot)}")
                    if not dryRun:
                        os.remove(dFname)
                        shutil.copyfile(sFname,dFname)
            bar()
                    
        # Delete files in dstNode that are not in srcNode
        for df in dstNode["files"]:
            dFname = pj(dstDirname,df["name"])
            sf = [f for f in srcNode["files"] if f["name"] == df["name"]]
            if not (fileInSrc := len(sf) > 0):
                print(f"- Removing file <dst>/{rp(dFname,dstRoot)}")
                if not dryRun: os.remove(dFname)

        # Delete directories in dstNode that are not in srcNode
        for dd in dstNode["directories"]:
            if len([d for d in srcNode["directories"] if d["name"] == dd["name"]]) == 0:
                print(f"- Removing directory <dst>/{rp(pj(dstDirname,dd['name']),dstRoot)}")
                if not dryRun: shutil.rmtree( pj(dstDirname,dd["name"]) )

        # If the dirs in srcNode are not in dstNode, create the dirs in dstNode.
        # Reiterate recursively in the directories in srcNode
        for sd in srcNode["directories"]:
            dd = [d for d in dstNode["directories"] if d["name"] == sd["name"]]
            dd = dd[0] if (dirInDst := len(dd) > 0 ) else None
            if dirInDst:
                core(sd,dd,pj(srcDirname,sd["name"]),pj(dstDirname,dd["name"]),dryRun,bar,dstRoot,srcRoot)
            else:
                print(f"- Adding directory <dst>/{rp(pj(dstDirname,sd['name']),dstRoot)}")
                if not dryRun:
                    copyTreeWithIgnores( srcRoot, pj(srcDirname,sd["name"]) , pj(dstDirname,sd["name"]) )

    print(f"Syncronizing {srcDirname} to {dstDirname}...")
    with alive_bar(srcTotCount,spinner="arrows") as bar:
        core(srcDB, dstDB, srcDirname, dstDirname, dryRun, bar)
        bar(srcTotCount)
    print("Re-updating dst database...")
    updateDatabase(dstDirname,srcTotCount)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="fsync",description="synchronizes two directories")
    parser.add_argument("src")
    parser.add_argument("dst")
    parser.add_argument("-d","--dry", action="store_true")
    parser.add_argument("--excludeFile",action="store",required=False)
    args = parser.parse_args()
    srcDir,dstDir,dryRun,excludeFile = args.src,args.dst,args.dry,args.excludeFile

    # Load excludeFile
    if excludeFile is not None:
        parseExcludeFile(excludeFile)

    if dryRun:
        print(f"This run will compare {srcDir} to {dstDir} and print the differences.")
    else:
        if input(f"This will sync {srcDir} to {dstDir}, continue?[Y/N]: ").lower() != "y": exit(0)
    syncDirectories(srcDir,dstDir,dryRun)
    input("Press ENTER to continue...")