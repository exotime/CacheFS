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

    def __init__(self, cache_dir, cache_size, path, charset="utf-8", flags = os.O_RDWR, node_id = None):
        self.cache_dir = cache_dir
        self.full_path = self.cache_file(path)
        try:
            os.makedirs(os.path.dirname(self.full_path))
        except OSError:
            pass

        self.path = path.decode(charset)
        self.cache_size = cache_size
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
                for nid,rah_active in db.execute('SELECT node_id,rah_active FROM file WHERE path = ?', (self.path,)):
                    self.node_id = nid
                    print "Hello!!!"
                    print rah_active

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

        print ">> %s Hits: %d, Misses: %d, Rate: %f%%" % (self.path, self.hits, self.misses, rate)

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
            return ["node_id = ? AND (offset = ? OR (offset < ? AND end >= ?))",
                    (self.node_id,
                     offset,
                     offset, offset + length)]
        else:
            return ["node_id = ? AND offset <= ? AND end > ?",
                    (self.node_id,            offset,     offset)]

    def __overlapping_block__(self, offset):
        return_offset = None
        return_size = None
        return_last = False

        conditions = self.__conditions__(offset)
        query = "select offset, end, last_block from file_blocks where %s" % conditions[0]
        db = open_db(self.cache_dir)
        with db:
            for db_offset, db_end, last_block in db.execute(query, conditions[1]):
                return_offset = db_offset
                return_size = (db_end - db_offset)
                return_last = last_block
        db.close()
        return (return_offset, return_size, return_last)

    def __add_block___(self, offset, length, last_bytes):
        insert_offset = offset
        insert_end = end = offset + length

        conditions = self.__conditions__(offset, length)
        query = "select min(offset), max(end) from file_blocks where %s" % conditions[0]

        db = open_db(self.cache_dir)
        with db:
            for db_offset, db_end in db.execute(query, conditions[1]):
                if db_offset == None or db_end == None:
                    break
                insert_offset = min(offset, db_offset)
                insert_end = max(end, db_end)

            db.execute("delete from file_blocks where %s" % conditions[0], conditions[1])
            db.execute('insert into file_blocks values (?, ?, ?, ?)', (self.node_id, insert_offset, insert_end, last_bytes))

        db.close()
        return

    def read(self, size, offset):
        # print ">>> READ (size: %s, offset: %s" % (size, offset)
        (addr, s, last_bytes) = self.__overlapping_block__(offset)
        if addr == None or (addr + s < offset + size and not last_bytes):
            self.misses += size
            raise CacheMiss
        os.lseek(self.cache, offset, os.SEEK_SET)
        buf = os.read(self.cache, size)
        self.hits += len(buf)
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


def File(file_system):
    class CacheFile(object):
    
        def __init__(self, path, flags, *mode):
            self.path = path
            self.pp = file_system._physical_path(self.path)
            self.size = os.stat(self.pp).st_size

            if len(mode) > 0:
                self.f = os.open(self.pp, flags, mode[0])
            else:
                self.f = os.open(self.pp, flags)

            inode_id = os.stat(self.pp).st_ino

            self.data_cache = FileDataCache(file_system.cache_dir, file_system.cache_size, path, file_system.charset, flags, inode_id)

        def read(self, size, offset):
            try:
                buf = self.data_cache.read(size, offset)                
            except CacheMiss:

                os.lseek(self.f, offset, os.SEEK_SET)
                buf = os.read(self.f, size)
                self.data_cache.update(buf, offset, offset + len(buf) == self.size, self.path)
            
            return buf
            

        def write(self, buf, offset):
            os.lseek(self.f, offset, os.SEEK_SET)
            os.write(self.f, buf)
            self.data_cache.update(buf, offset, offset + len(buf) == self.size, self.path)
            return len(buf)


        def release(self, flags):
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
        phys_resolved = os.readlink(self._physical_path(path))
        debug('   resolves to physical "%s"' % phys_resolved)
        return phys_resolved


    def unlink(self, path):
        print('>> unlink("%s")' % path)
        os.remove(self._physical_path(path))
        try:
            FileDataCache(self.cache_dir, self.cache_size, path, self.charset).unlink()
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
        FileDataCache.rmdir(self.cache_dir, path)

    def symlink(self, target, name):
        print('>> symlink("%s", "%s")' % (target, name))
        os.symlink(self._physical_path(target), self._physical_path(name))

    def link(self, target, name):
        print('>> link(%s, %s)' % (target, name))
        os.link(self._physical_path(target), self._physical_path(name))
        FileDataCache(self.cache_dir, self.cache_size, name, self.charset, None, os.stat(self._physical_path(name)).st_ino)

    def rename(self, old_name, new_name):
        print('>> rename(%s, %s)' % (old_name, new_name))
        os.rename(self._physical_path(old_name),
                  self._physical_path(new_name))
        try:
            fdc = FileDataCache(self.cache_dir, self.cache_size, old_name, self.charset)
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
            cache = FileDataCache(self.cache_dir, self.cache_size, path, self.charset)
            cache.open()
            cache.truncate(len)
            cache.close()
        except:
            pass


def open_db(cache_dir):
    return sqlite3.connect(os.path.join(cache_dir, "metadata.db"), isolation_level="DEFERRED")

def create_db(cache_dir):
    cache_db = open_db(cache_dir)

    cache_db.execute("""
        CREATE TABLE IF NOT EXISTS file (
            id          INTEGER NOT NULL, 
            node_id     INTEGER,
            path        STRING,
            last_use    INTEGER,
            open        BOOLEAN DEFAULT 1,
            rah_active  BOOLEAN DEFAULT 0,
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

    cache_db.execute("PRAGMA synchronous=NORMAL")
    cache_db.execute("PRAGMA journal_mode=WAL")
    cache_db.close()

def print_help():
    print """
 -- Required Parameters --
  mount point : Path to mount cached directory
  target      : Path to be cached

 -- Optional Parameters --

  cache_dir   : Path to place the cached files
    -- Defaults to ~/.cachefs if not provided
  cache_size  : Size of the cache in Mega Bytes
    -- Defaults to 1024 (1 GB)
  charset     : Charset of the target files system
    -- Defaults to utf-8

 Format:
  cachefs.py [mount point] -o target=[path],cache_dir=[path],[...etc] -o [other fuse options]

 Example:
  cachefs.py /home/joe/CachedFolder -o target=/home/joe/FolderToCache,cache_dir=/home/joe/CachedFiles,cache_size=7168 -o allow_other
    """
    sys.exit()

def main():

    usage='%prog MOUNTPOINT -o target=SOURCE cache=SOURCE [options]'
    server = CacheFS(version='CacheFS %s' % CACHE_FS_VERSION,
                     usage=usage,
                     dash_s_do='setsingle')

    server.parser.add_option(
        mountopt="cache_dir", metavar="PATH",
        default=None,
        help="Path to place the cache")

    server.parser.add_option(
        mountopt="cache_size", metavar="N",
        default=1 * 1024 * 1024 * 1024,
        help="Size of the cache in Mega Bytes")

    server.parser.add_option(
        mountopt="charset", metavar="PATH CHARSET",
        default="utf-8",
        help="Charset of the target files system")

    # Wire server.target to command line options
    server.parser.add_option(
        mountopt="target", metavar="PATH",
        default=None,
        help="Path to be cached")


    server.parse(values=server, errex=1)

    server.target = os.path.abspath(server.target)
    server.multithreaded = 1

    try:
        server.cache_size = int(server.cache_size) * (1024 * 1024)
    except AttributeError:
        server.cache_size = 1 * 1024 * 1024 * 1024

    try:
        cache_dir = server.cache_dir
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
    print '  Target Dir     : %s' % server.target
    print '  Target Charset : %s' % server.charset
    print '  Cache Dir      : %s' % server.cache_dir
    print '  Cache Max Size : %s' % server.cache_size
    print '  Mount Point    : %s' % os.path.abspath(server.fuse_args.mountpoint)
    print
    print 'Unmount through:'
    print '  fusermount -u %s' % server.fuse_args.mountpoint
    print
    print 'Done.'
    server.main()

if __name__ == '__main__':
    for arg in sys.argv:
        print arg
        if arg == '--help':
            print_help()
            exit()
        if arg == '-h':
            print_help()
            exit()
    main()
