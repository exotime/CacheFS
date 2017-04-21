CacheFS
=======
Caching your files so you don't have to.

Project Status
--------------
This is very early code.  

I've done some testing, and it appears to not corrupt your files.  It's very far from feature complete, and I wouldn't trust it for important files.

Quick Start (Ubuntu)
-----------
Install libfuse2 and python bindings

    sudo apt-get install libfuse2 python-fuse python-pysqlite2

Grab the latest CacheFS

    git clone git@github.com:cfoucher/CacheFS.git


Start caching

    ./CacheFS/cachefs.py [mount point] -o target=[slow drive]

Thats it!

Usage
-----
    cachefs.py [mount point] -o target=[slow drive],cache_dir=[path],[...etc] -o [other fuse options]

mount point:  The directory you want to mount cachefs to.  Interacting with files in this directory after mounting a volume will use cachefs.

target:  This is the volume or directory you wish to cache.  

cache_dir:  This optional argument specifies where you wish the cache to be stored.  If it is not specified a place will be created for you in your home directory.

cache_size: This optional argument specifies the max amount of data to be cached. This is a soft limit i.e. cache can expand beyond this limit if multiple files are open at once, exceeding this value. This is set in MB, defaulting to 1024 MB (1GB) if not specified.


Why
----
Storage is typically either fast and small (SSD), fast-ish and large-ish (Spinning Disks), or very large and very slow (s3fs, sshfs, etc).  Computers use caching to keep frequently used data closer to the CPU, and have for decades.  The goal with CacheFS is to use the same principles that make RAM appear as fast as L2 to a CPU and make large/slow volumes appear as fast as small/fast volumes.

What
----
CacheFS is a FUSE file system that acts as a local mirror for files on a large/slow drive.  Running under the assumption that bigger disks are slower, it attempts to keep a copy of your file data on a small/fast disk, without losing the drive capacity of the large/slow disk.

This is not a [dropbox](http://www.dropbox.com "Dropbox") replacement.  I love dropbox and use it frequently.  The ultimate goal is to have a volume that has unlimited storage capacity (say, s3fs) that acts as fast as your local drive.  It could also help with laptops that have tiny SSDs and an external drive.  With the addition of an offline mode you could have access to the files on your external drive while away from home if they happen to be cached.

You could theoreticaly use this to create a hierarchy of volumes from a ramdrive through an SSD, Spinning disk, and cloud storage.  I'm not sure that this would gain you anything, but it sounds cool!

To Be Implemented
------------------

The time to complete write operations (or any fs modifications) is the time it takes to modify both the slow disk and the fast disk.  This needs to be changed so that modification operations happen on the cache, and then go to the slow disk in the background.  

There is no dashboard or other way to look into the health/status of a cachefs mountpoint.

Little/no multi-user safety.  If multiple people use cachefs to cache the same files, it is not guaranteed that changes made by one person will percolate to the other person.

No offline mode.


Limitations
-----------
CacheFS (as implemented with python-fuse) will never be as fast as your small/fast disk.  The goal is to be significantly faster than the large/slow disk.  

FUSE will probably never be ported to Windows, so CacheFS will probably never work in windows.  I have not been able to get it to work in MacOS X, but I haven't tried very hard.

