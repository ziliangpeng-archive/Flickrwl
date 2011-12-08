import threading, os


printMutex = threading.Lock()

dirLock = threading.Lock()


def syncPrint(s):
    with printMutex:
        print s


def create_path(path, relative=True):
    if relative:
        subpath = relative and '.' or ''
    with dirLock:
        for dir in path.split('/')[:-1]:
            subpath += '/' + dir
            if not os.path.exists(subpath):
                os.mkdir(subpath)
            if not os.path.isdir(subpath):
                os.rename(subpath, subpath + '_backup')
                os.mkdir(subpath)
