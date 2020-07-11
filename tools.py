import os
from settings import tag_typer

def getFilesToDelete(self, newFileName):
    dirName, fName = os.path.split(self.origFileName)
    dName, newFileName = os.path.split(newFileName)

    fileNames = os.listdir(dirName)
    result = []
    prefix = fName + "."
    postfix = self.postfix
    prelen = len(prefix)
    postlen = len(postfix)
    for fileName in fileNames:
        if fileName[:prelen] == prefix and fileName[-postlen:] == postfix and len(
                fileName) - postlen > prelen and fileName != newFileName:
            suffix = fileName[prelen:len(fileName) - postlen]
            if self.extMatch.match(suffix):
                result.append(os.path.join(dirName, fileName))
    result.sort()
    if len(result) < self.backupCount:
        result = []
    else:
        result = result[:len(result) - self.backupCount]
    return result

async def work_files_loop():

    while True:
        opc_queue_logger.warning( f"constants_loaded: {constants_filled} ", )
        if not constants_filled:
            await get_constants()
        else:
            return
