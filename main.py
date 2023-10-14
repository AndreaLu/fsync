import os,shutil                     # filesystem management
import utils                         # DB management functions
import sys                           # input arguments
from alive_progress import alive_bar # loading bar

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

"""
If dirname already has a database, update it, otherwise, create it.
The database holds information about the size, hash, name, date of last edit
for every file in the directory.
"""
def updateDatabase(dirname,totCount):
    def core(root, dirname, bar, isRoot=True):
        # Add or update records of existing files
        for f in os.listdir(dirname):
            fname = pj(dirname, f)
            if not os.path.isfile(fname): continue 
            if isRoot and f == "fsync.db": continue 
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

        # Delete records of files no longer existing
        toRemove = []
        for i in range(len(root["files"])):
            f = root["files"][i]
            if not os.path.isfile(pj(dirname, f["name"])):
                toRemove.append(i-len(toRemove))
        for i in toRemove:
            del root["files"][i]
        toRemove.clear()

        # Delete the records of directories no longer existing
        for i in range(len(root["directories"])):
            d = root["directories"][i]
            if not os.path.isdir(pj(dirname, d["name"])):
                toRemove.append(i-len(toRemove))
        for i in toRemove:
            del root["directories"][i]

        # Adds records of directories not present in DB and recursively calls back this function
        # in the subdirectories of root
        for f in os.listdir(dirname):
            xxx = pj(dirname, f)
            if not os.path.isdir(xxx):
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
            core(newRoot, pj(dirname, f), bar, False)

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
    def core(srcNode, dstNode, srcDirname, dstDirname, dryRun, bar, dstRoot=None):
        if dstRoot is None: dstRoot = dstDirname
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

        # Reiterate recursively in the directories in srcNode that are already in dstNode
        # Copy the directories in srcNode that are not in dstNode.
        for sd in srcNode["directories"]:
            dd = [d for d in dstNode["directories"] if d["name"] == sd["name"]]
            dd = dd[0] if (dirInDst := len(dd) > 0 ) else None
            if dirInDst:
                core(sd,dd,pj(srcDirname,sd["name"]),pj(dstDirname,dd["name"]),dryRun,bar,dstRoot)
            else:
                print(f"- Adding directory <dst>/{rp(pj(dstDirname,sd['name']),dstRoot)}")
                if not dryRun:
                    shutil.copytree(  pj(srcDirname,sd["name"]) , pj(dstDirname,sd["name"]) )

    print(f"Syncronizing {srcDirname} to {dstDirname}...")
    with alive_bar(srcTotCount,spinner="arrows") as bar:
        core(srcDB, dstDB, srcDirname, dstDirname, dryRun, bar)
        bar(srcTotCount)
    print("Re-updating dst database...")
    updateDatabase(dstDirname,srcTotCount)


if __name__ == "__main__":
    if len(sys.argv) != 3 and len(sys.argv) != 4:
        print("Wrong usage. Please use like this: `fsync <src> <dst>`")
    srcDir,dstDir = sys.argv[1],sys.argv[2]
    dryRun = "--dry" in sys.argv
    if dryRun: print("This will compare {srcDir} to {dstDir} and print the differences.")
    else:
        ans = input(f"This will sync {srcDir} to {dstDir}, continue?[Y/N]: ")
        if ans.lower() == "y":
            syncDirectories(sys.argv[1],sys.argv[2],dryRun)
        else:
            print("Doing nothing...")
    input("Press ENTER to continue...")