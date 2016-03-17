#!/usr/bin/env python

import time
import os
import stat
import errno
import sys
import sqlite3
import pprint
import logging
import hashlib
from multiprocessing import Value
from threading import Thread, Event, active_count, Lock
from Queue import Queue, Empty

CACHE_FS_VERSION = '0.0.1'
import fuse
fuse.fuse_python_api = (0, 2)

log_file = sys.stdout

def debug(text):
    pass
#    log_file.write(text)
#    log_file.write('\n')
#    log_file.flush()

class CacheMiss(Exception):
    def __init__(self):
        debug(">> CACHE MISS")
    pass

class FileDataCache:
    def cache_file(self, path):
        return os.path.normpath(os.path.join(self.cache_dir, "file_data") + path)

    def __init__(self, cache_dir, cache_size, path, db_end, charset="utf-8", flags = os.O_RDWR, node_id = None):
        self.cache_dir = cache_dir
        self.full_path = self.cache_file(path)
        try:
            os.makedirs(os.path.dirname(self.full_path))
        except OSError:
            pass

        self.path = path.decode(charset)
        self.cache_size = cache_size
        self.db_end = db_end
        self.cache = None
        self.flags = os.O_RDWR | ( flags &
                                   os.O_CREAT &
                                   os.O_EXCL )
        self.node_id = node_id
        self.open()

        if flags & os.O_TRUNC:
            self.truncate(0)

        db = open_db(self.cache_dir)
        with db:
            if self.node_id != None:
                db.execute('INSERT OR REPLACE INTO file (node_id,path,last_use) values (?,?,?)', (self.node_id,self.path,time.time()))
            else:
                for nid, in db.execute('SELECT node_id FROM file WHERE path = ?', (self.path,)):
                    self.node_id = nid

                if self.node_id == None:
                    print "Unable to find path in db and no node_id given, unable to open cache"
                    raise CacheMiss
                    
            for other_path, in db.execute('SELECT path FROM file WHERE node_id = ? AND path != ?', (self.node_id, self.path)):
                try:
                    if not os.path.exists(self.full_path) and os.path.exists(self.cache_file(other_path)):
                        os.link(self.cache_file(other_path), self.full_path)
                except Exception, e:
                    print "link error: %s" % e
                    raise e
        db.close()

        self.misses = 0
        self.hits = 0

    def known_offsets(self):
        ret = {}
        db = open_db(self.cache_dir)
        with db:
            for offset, end in db.execute('select offset, end from file_blocks where node_id = ?', (self.node_id,)):
                ret[offset] = end - offset
        return ret
        db.close()

    def open(self):
        if self.cache == None:
            try:
                self.cache = os.open(self.full_path, self.flags)
            except Exception, e:
                self.cache = os.open(self.full_path, self.flags | os.O_CREAT )

    def report(self):
        rate = 0.0
        if self.hits + self.misses:
            rate = 100*float(self.hits)/(self.hits + self.misses)

        #pprint.pprint(self.known_offsets())
        #print ">> %s Hits: %d, Misses: %d, Rate: %f%%" % (self.path, self.hits, self.misses, rate)

    def __del__(self):
        self.close()

    def close(self):
        if self.cache:
            os.close(self.cache)
            self.cache = None
        db = open_db(self.cache_dir)
        with db:
            db.execute("UPDATE file SET open = 0 WHERE node_id = ?", (self.node_id,))
        db.close()

    def __conditions__(self, offset, length=None):
        if length != None:
            return ["node_id = ? AND (offset = ? OR "
                    "(offset > ? AND offset <= ?) OR (offset < ? AND end >= ?))",
                    (self.node_id,
                     offset,
                     offset, offset + length,
                     offset, offset)]
        else:
            return ["node_id = ? AND offset <= ? AND end > ?",
                    (self.node_id,            offset,     offset)]

    def __overlapping_block__(self, offset):
        conditions = self.__conditions__(offset)
    
        query = "select offset, end, last_block from file_blocks where %s" % conditions[0]
        db = open_db(self.cache_dir)
        with db:
            for db_offset, db_end, last_block in db.execute(query, conditions[1]):
                db.close()
                return (db_offset, db_end - db_offset, last_block)

        db.close()
        return (None, None, False)

    def __add_block___(self, offset, length, last_bytes):
        end = offset + length

        conditions = self.__conditions__(offset, length)
        query = "select min(offset), max(end) from file_blocks where %s" % conditions[0]
        db = open_db(self.cache_dir)
        with db:
            for db_offset, db_end in db.execute(query, conditions[1]):
                if db_offset == None or db_end == None:
                    continue
                offset = min(offset, db_offset)
                end = max(end, db_end) 

            db.execute("delete from file_blocks where %s" % conditions[0], conditions[1])
            db.execute('insert into file_blocks values (?, ?, ?, ?)', (self.node_id, offset, end, last_bytes))

        db.close()

        # Update db_end value for read a head process
        self.db_end.value = end
        return

    def read(self, size, offset):
        #print ">>> READ (size: %s, offset: %s" % (size, offset)
        (addr, s, last_bytes) = self.__overlapping_block__(offset)
        if addr == None or (addr + s < offset + size and not last_bytes):
            self.misses += size
            if addr is None and s is None:
                self.db_end.value = offset
            elif addr is None and s is not None:
                self.db_end.value = s
            else:
                self.db_end.value = (addr + s)
            raise CacheMiss

        os.lseek(self.cache, offset, os.SEEK_SET)
        buf = os.read(self.cache, size)
        self.hits += len(buf)
        
        # Update db_end value for read a head process
        self.db_end.value = (addr + s)
        return buf

    def update(self, buff, offset, last_bytes=False, path=None):
        #print ">>> UPDATE (len: %s, offset, %s)" % (len(buff), offset)
        needed_size = len(buff)
        self._make_room(needed_size)
        os.lseek(self.cache, offset, os.SEEK_SET)
        os.write(self.cache, buff)
        os.fsync(self.cache)
        self.__add_block___(offset, len(buff), last_bytes)

    def _current_cache_size(self):
        result = 0
        db = open_db(self.cache_dir)
        with db:
            for size, in db.execute("SELECT SUM(end - offset) AS size FROM file_blocks"):
                if size:
                    result = size
        db.close()
        return result

    def _make_room(self, needed_size):
        cache_size = self._current_cache_size()
        db = open_db(self.cache_dir)
        with db:
            if (cache_size + needed_size) > self.cache_size:
                node_id_to_delete = []
                
                for size, last_use, node_id in db.execute("SELECT SUM(b.end - b.offset) AS size, f.last_use, f.node_id FROM file_blocks as b, file as f WHERE f.node_id = b.node_id AND f.node_id != ? AND f.open = 0 GROUP BY f.node_id ORDER BY f.last_use ASC;", (self.node_id,)):
                    cache_size -= size
                    node_id_to_delete.append(node_id)
                    if cache_size + needed_size <= self.cache_size:
                        break

                for node_id in node_id_to_delete:
                    for path, in db.execute("SELECT path FROM file WHERE node_id = ?", (node_id,)):
                        path = self.cache_file(path.encode("utf-8"))
                        if path.startswith(self.cache_dir) and os.path.exists(path):
                            print "I am able to remove it!"
                            print path
                            os.remove(path)

                    db.execute("DELETE FROM file WHERE node_id = ?", (node_id,))
                    db.execute("DELETE FROM file_blocks WHERE node_id = ?", (node_id,))
        db.close()

    def truncate(self, l):
        #print ">>> TRUNCATE (cache: %s, len: %s)" % (self.cache, l)
        try:
            os.ftruncate(self.cache, l)

            db = open_db(self.cache_dir)
            with db:
                db.execute("DELETE FROM file_blocks WHERE node_id = ? AND offset >= ?", (self.node_id, l))
                db.execute("UPDATE file_blocks SET end = ? WHERE node_id = ? AND end > ?", (l, self.node_id, l))
            db.close()
        except Exception, e:
            print "Error truncating: %s" % e
        
        return

    def unlink(self):
        os.remove(self.full_path)
        db = open_db(self.cache_dir)
        with db:
            db.execute("DELETE FROM file WHERE path = ?", self.path)
            count = db.execute("SELECT COUNT(*) FROM file WHERE path = ? ", self.path).fetchone()[0]
            if count == 0:
                db.execute("DELETE FROM file_blocks WHERE node_id = ?", (self.node_id,))
        db.close()

    @staticmethod
    def rmdir(cache_base, path):
        d = os.path.join(cache_base, "file_data") + path
        try:
            os.rmdir(d)
        except:
            pass

    def rename(self, new_name):
        db = open_db(self.cache_dir)
        with db:
            db.execute("INSERT OR REPLACE INTO file (path) values (?)", (self.path,))
        db.close()

        new_full_path = self.cache_file(new_name)
            
        try:
            os.makedirs(os.path.dirname(new_full_path))
        except OSError:
            pass

        os.rename(self.full_path, new_full_path)



#class ReadAHead(Process):
class ReadAHead(Thread):    
    def __init__(self, file_system, path, flags, mode, offset, db_end, size, read_a_head_error, read_a_head_queue):
        # Need for class to be properly initialized as a Process subclass
        Thread.__init__(self)

        # Variables needed in read a head process
        self.exit = Event()
        self.lock = Lock()
        self.file_system = file_system
        self.path = path
        self.flags = flags
        self.mode = mode
        self.size = size
        self.pp = file_system._physical_path(self.path)
        self.inode_id = os.stat(self.pp).st_ino
        self.offset = offset
        self.db_end = db_end
        self.read_a_head_error = read_a_head_error
        self.read_a_head_queue = read_a_head_queue

        # Place holders. Can't initialize until the process is "started" or we will leave orphaned file descriptors in main process 
        self.rah_f = None
        self.rah_db = None
        self.rah_data_cache = None

    def run(self):
        # Log output from process to file - for debuging
        #sys.stdout = open("/home/conrad/cacheFS/thread.log", "a", buffering=0)
        #sys.stderr = open("/home/conrad/cacheFS/thread.log", "a", buffering=0)

        extra_print = False
        if len(self.mode) > 0:
            self.rah_f = os.open(self.pp, self.flags, self.mode[0])
        else:
            self.rah_f = os.open(self.pp, self.flags)

        self.rah_db = sqlite3.connect(os.path.join(self.file_system.cache, "metadata.db"), isolation_level="Immediate", timeout=1.0)
        self.rah_data_cache = FileDataCache(self.rah_db, self.file_system.cache, self.file_system.cache_size, self.path, self.db_end, self.file_system.charset, self.flags, self.inode_id, True)

        while not self.exit.is_set():

            if os.getppid() == 1:
                print "Parent process has terminated"
                break

            if extra_print:
                extra_print = False
                print "Print after negative"
                print "db_end = %s" % self.db_end.value
                print "offest = %s" % self.offset.value

            print "Buffer = %s MB" % ((long(self.db_end.value) - long(self.offset.value))/long(1048576))
            if (long(self.db_end.value) - long(self.offset.value)) < 0:
                print "Less than zero"
                print "db_end = %s" % self.db_end.value
                print "offest = %s" % self.offset.value
                extra_print = True

            if 0 <= (long(self.db_end.value) - long(self.offset.value)) <= 52428800 and long(self.db_end.value) < long(self.size):
                #print "Reading a head - %s" % os.getpid()
                try:
                    os.lseek(self.rah_f, self.db_end.value, os.SEEK_SET)
                    buf = os.read(self.rah_f, 131072)
                    # If offset == to db_end, read has either caught up to the read_a_head process or this is the first access.
                    # Return the data before writing it to cache to improve read speed.
                    if long(self.offset.value) >= long(self.db_end.value):
                        #print "Trying to add to queue"
                        self.read_a_head_queue.put([self.db_end.value, buf])
                        #print "Added!"
                    self.lock.acquire()
                    self.rah_data_cache.update(buf, self.db_end.value, self.db_end.value + len(buf) == self.size, self.path)
                    self.lock.release()
                except OSError as e:
                    print "Error = %s" %e
                    self.read_a_head_error.value = True

                #print "read a head done"
            else:
                # Wait to prevent process from using all the CPU resources
                #print "sleeping - %s" % os.getpid()
                time.sleep(0.05)

        self.release()
        sys.exit()
        return

    def shutdown(self):
        #print "Shutdown initiated"
        self.exit.set()

    def release(self):
        #print "Releasing"
        os.close(self.rah_f)
        self.rah_data_cache.close()
        self.rah_db.close()
        return      


def File(file_system):
    class CacheFile(object):
        direct_io = False
        keep_cache = False
    
        def __init__(self, path, flags, *mode):
            self.path = path
            self.pp = file_system._physical_path(self.path)
            #print('>> file<%s>.open(flags=%d, mode=%s)' % (self.pp, flags, mode))

            if len(mode) > 0:
                self.f = os.open(self.pp, flags, mode[0])
            else:
                self.f = os.open(self.pp, flags)

            inode_id = os.stat(self.pp).st_ino

            # Shared variable for read a head thread
            self.read_a_head_enabled = False
            self.read_a_head_queue = Queue()
            self.read_a_head_error = Value('b', False)
            self.offset = Value('L', 0)
            self.db_end = Value('L', 0)
            self.size = os.stat(self.pp).st_size

            self.db = open_db(file_system.cache_dir)
            self.data_cache = FileDataCache(file_system.cache_dir, file_system.cache_size, path, self.db_end, 
                file_system.charset, flags, inode_id)

            # Start parallel thread for Read A Head
            #self.read_a_head_process = ReadAHead(file_system, path, flags, mode, self.offset, self.db_end, 
            #    self.size, self.read_a_head_error, self.read_a_head_queue)
            #self.read_a_head_process.daemon = True
            #self.read_a_head_process.start()

        def read(self, size, offset, recursion=0):

            if offset >= self.size:
                # Let Fuse handle the eventual error
                os.lseek(self.f, offset, os.SEEK_SET)
                buf = os.read(self.f, size)
                return buf
            else:
                self.offset.value = offset

            # Check and see if the read_a_head thread put the first request in the queue
            #if offset >= self.db_end.value and recursion > 0:
            #    # First time reading this part of file wait for data from async thread
            #    print "first access at %s. Waiting for data" % offset
            #    try:
            #        buf = self.read_a_head_queue.get(block=True, timeout=2)
            #        #pprint.pprint(buf[0])
            #        if buf[0] != offset:
            #            print "queue has wrond data - recursion #%s" % recursion
            #            recursion+=1
            #            buf = self.read(size, offset, recursion)
            #    except Empty:
            #        print "queue timeout on recursion #%s" % recursion
            #        recursion+=1
            #        buf = self.read(size, offset, recursion)
            #        
            #    return buf[1][:size]

            try:
                buf = self.data_cache.read(size, offset)                
            except CacheMiss:
                if self.read_a_head_error.value:
                    print "Hello"
                if self.read_a_head_enabled and not self.read_a_head_error.value:
                    # Small wait to give the read a head thread time to grab the data
                    # and prevent running into the recursion limit
                    recursion+=1
                    time.sleep(0.05)
                    buf = self.read(size, offset, recursion)
                else:
                    self.read_a_head_error.value = False
                    os.lseek(self.f, offset, os.SEEK_SET)
                    buf = os.read(self.f, size)
                    self.data_cache.update(buf, offset, offset + len(buf) == self.size, self.path)
            
            #print "hit - db_end = %s" % self.db_end.value
            return buf
            

        def write(self, buf, offset):
            print('>> file<%s>.write(len(buf)=%d, offset=%s)' % (self.path, len(buf), offset))
            os.lseek(self.f, offset, os.SEEK_SET)
            os.write(self.f, buf)

            self.data_cache.update(buf, offset, offset + len(buf) == self.size, self.path)

            return len(buf)


        def release(self, flags):
            sys.stdout.flush()
            os.close(self.f)
            self.data_cache.close()
            self.data_cache.report()
            return 0

        def flush(self):
            os.fsync(self.f)

    return CacheFile



class CacheFS(fuse.Fuse):
    def __init__(self, *args, **kwargs):
        fuse.Fuse.__init__(self, *args, **kwargs)
        self.file_class = File(self)

    def _physical_path(self, path):
        phys_path = os.path.join(self.target, path.lstrip('/'))
        return phys_path
    
    def getattr(self, path):
        try:
           pp = self._physical_path(path)
           # Hide non-public files (except root)

           return os.lstat(pp)
        except Exception, e:
           debug(str(e))
           raise e

    def readdir(self, path, offset):
        phys_path = self._physical_path(path).rstrip('/') + '/'
        for r in ('..', '.'):
            yield fuse.Direntry(r)
        for r in os.listdir(phys_path):
            virt_path = r
            debug('readdir yield: ' + virt_path)
            yield fuse.Direntry(virt_path)

    def readlink(self, path):
        #print('>> readlink("%s")' % path)
        phys_resolved = os.readlink(self._physical_path(path))
        debug('   resolves to physical "%s"' % phys_resolved)
        return phys_resolved


    def unlink(self, path):
        print('>> unlink("%s")' % path)
        os.remove(self._physical_path(path))
        try:
            FileDataCache(self.cache_db, self.cache, self.cache_size, path, self.charset).unlink()
        except:
            pass
        return 0

    # Note: utime is deprecated in favour of utimens.
    def utime(self, path, times):
        """
        Sets the access and modification times on a file.
        times: (atime, mtime) pair. Both ints, in seconds since epoch.
        Deprecated in favour of utimens.
        """
        debug('>> utime("%s", %s)' % (path, times))
        os.utime(self._physical_path(path), times)
        return 0

    def access(self, path, flags):
        path = self._physical_path(path)
        os.access(path, flags)

    def mkdir(self, path, mode):
        print('>> mkdir("%s")' % path)
        path = self._physical_path(path)
        os.mkdir(path, mode)
        
    def rmdir(self, path):
        print('>> rmdir("%s")' % path)
        os.rmdir( self._physical_path(path) )
        FileDataCache.rmdir(self.cache, path)

    def symlink(self, target, name):
        print('>> symlink("%s", "%s")' % (target, name))
        os.symlink(self._physical_path(target), self._physical_path(name))

    def link(self, target, name):
        print('>> link(%s, %s)' % (target, name))
        os.link(self._physical_path(target), self._physical_path(name))
        FileDataCache(self.cache_db, self.cache, self.cache_size, name, self.charset, None, os.stat(self._physical_path(name)).st_ino)

    def rename(self, old_name, new_name):
        print('>> rename(%s, %s)' % (old_name, new_name))
        os.rename(self._physical_path(old_name),
                  self._physical_path(new_name))
        try:
            fdc = FileDataCache(self.cache_db, self.cache, self.cache_size, old_name, self.charset)
            fdc.rename(new_name)
        except :
            pass
        
    def chmod(self, path, mode):
        os.chmod(self._physical_path(path), mode)
        
    def chown(self, path, user, group):
        os.chown(self._physical_path(path), user, group)
	
    def truncate(self, path, len):
        f = open(self._physical_path(path), "a")
        f.truncate(len)
        f.close()
        try:
            cache = FileDataCache(self.cache_db, self.cache, self.cache_size, path, self.charset)
            cache.open()
            cache.truncate(len)
            cache.close()
        except:
            pass


def open_db(cache_dir):
    return sqlite3.connect(os.path.join(cache_dir, "metadata.db"), isolation_level="Immediate")

def create_db(cache_dir):
    cache_db = open_db(cache_dir)

    cache_db.execute("""
        CREATE TABLE IF NOT EXISTS file (
            id          INTEGER NOT NULL, 
            node_id     INTEGER,
            path        STRING,
            last_use    INTEGER,
            open        BOOLEAN DEFAULT 1,
            FOREIGN KEY(node_id) REFERENCES nodes(id)
            UNIQUE(path),
            PRIMARY KEY(id)
        )
    """)

    cache_db.execute("""
        CREATE TABLE IF NOT EXISTS file_blocks (
            node_id    INTEGER NOT NULL,
            offset     INTEGER,
            end        INTEGER,
            last_block BOOLEAN DEFAULT 0,
            FOREIGN KEY(node_id) REFERENCES file(node_id)
        )
    """)

    cache_db.execute("PRAGMA synchronous=OFF")
    cache_db.execute("PRAGMA journal_mode=OFF")
    cache_db.close()

def print_help():
    print """
    Help Text Here
    """
    sys.exit()

def main():

    usage='%prog MOUNTPOINT -o target=SOURCE cache=SOURCE [options]'
    server = CacheFS(version='CacheFS %s' % CACHE_FS_VERSION,
                     usage=usage,
                     dash_s_do='setsingle')

    server.parser.add_option(
        mountopt="cache", metavar="PATH",
        default=None,
        help="Path to place the cache")

    server.parser.add_option(
        mountopt="cache_size", metavar="N",
        default=1 * 1024 * 1024 * 1024,
        help="size of the cache in bytes")

    server.parser.add_option(
        mountopt="charset", metavar="PATH CHARSET",
        default="utf-8",
        help="charset of the target files system")

    # Wire server.target to command line options
    server.parser.add_option(
        mountopt="target", metavar="PATH",
        default=None,
        help="Path to be cached")


    server.parse(values=server, errex=1)

    server.target = os.path.abspath(server.target)
    server.multithreaded = 1

    try:
        server.cache_size = int(server.cache_size)
    except AttributeError:
        server.cache_size = 100 * 1024 * 1024 #5 * 1024 * 1024 * 1024

    try:
        cache_dir = server.cache
    except AttributeError:
        try:
            os.mkdir(os.path.join(os.path.expanduser("~"), ".cachefs"))
        except OSError:
            pass
        cache_dir = os.path.join(os.path.expanduser("~"), ".cachefs", hashlib.md5(server.target).hexdigest())
    
    create_db(cache_dir)
    server.cache_dir = os.path.abspath(cache_dir)

    try:
        os.mkdir(server.cache_dir)
    except OSError:
        pass
    
    try:
        server.charset
    except AttributeError:
        server.charset = "utf-8"

    print 'Setting up CacheFS %s ...' % CACHE_FS_VERSION
    print '  Target         : %s' % server.target
    print '  Target charset : %s' % server.charset
    print '  Cache          : %s' % server.cache_dir
    print '  Cache max size : %s' % server.cache_size
    print '  Mount Point    : %s' % os.path.abspath(server.fuse_args.mountpoint)
    print
    print 'Unmount through:'
    print '  fusermount -u %s' % server.fuse_args.mountpoint
    print
    print 'Done.'
    server.main()

if __name__ == '__main__':
    for arg in sys.argv:
        if arg == '--help':
            print_help()
        if arg == '-h':
            print_help()
    main()
