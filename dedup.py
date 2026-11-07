#!/usr/bin/env python3
"""dedup - merge old backup drives into one target, copying only content the target lacks.

Workflow, once per source drive:

    dedup.py index   TARGET                 # optional: build/refresh the target index
    dedup.py compare TARGET SOURCE          # scan source, print report, write copy plan
    dedup.py import  TARGET SOURCE          # copy planned files to TARGET/_import/<label>/
    dedup.py check   TARGET SOURCE          # is every source file's content on the target?

Files are compared by content (BLAKE2b-128), never by name or timestamp. Only files whose
size matches something are hashed, first partially (head+tail), then fully.

The target index lives in TARGET/.dedup/. compare/import/check always refresh it first
(a stat walk; only new/changed files are re-hashed later, and only when needed).
The tool never deletes or modifies anything outside TARGET/.dedup and TARGET/_import.
Python stdlib only.
"""

import argparse
import errno
import fcntl
import fnmatch
import hashlib
import json
import os
import queue
import re
import shutil
import signal
import sqlite3
import stat
import sys
import threading
import time
from collections import Counter, namedtuple
from concurrent.futures import ThreadPoolExecutor

CHUNK = 64 * 1024            # partial hash reads this much from head and tail
FULL_READ_MAX = 2 * 1024 * 1024  # files up to this size are read in full instead of head + tail:
                             # on a HDD one seek plus 2 MB of transfer costs about as much as two seeks
BUF = 1024 * 1024            # copy / full-hash buffer
DEDUP_DIR = b'.dedup'
IMPORT_DIR = b'_import'
PARTIAL_SUFFIX = b'.dedup-partial'
FOLDERS = {'new': b'new', 'older': b'older_versions', 'newer': b'newer_on_source'}
COPY_CATS = tuple(FOLDERS)
SCAN_MAX_AGE = 3600          # compare warns when reusing a source scan older than this
MTIME_TOL_NS = 2_000_000_000  # FAT has 2 s timestamp resolution
BLOCK = 4096
DEFAULT_SOURCE_EXCLUDES = [
    'System Volume Information', '$RECYCLE.BIN', 'RECYCLER', '.Trash-*', '.Trashes',
    '.Spotlight-V100', '.fseventsd', 'lost+found',
]
CAT_LABEL = {
    'present': 'already on target',
    'dup': 'dup within source',
    'new': 'new',
    'older': 'older_versions',
    'newer': 'newer_on_source',
    'error': 'unreadable',
}


# ----------------------------------------------------------------------------- helpers

STOP = threading.Event()   # set by Ctrl-C / SIGTERM / SIGHUP; long loops check it
RUN = {'store': None, 'cmd': None, 'reindexed': False}


class Stopped(KeyboardInterrupt):
    pass


def on_signal(signum, frame):
    if STOP.is_set():
        sys.stderr.write('\nquitting immediately\n')
        os._exit(130)
    STOP.set()
    sys.stderr.write('\nstopping — saving progress (press Ctrl-C again to quit immediately) …\n')
    raise Stopped

def die(msg):
    sys.stderr.write(f'error: {msg}\n')
    sys.exit(2)


def hsize(n):
    n = float(n)
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(n) < 1000 or unit == 'TB':
            return f'{int(n)} B' if unit == 'B' else f'{n:.1f} {unit}'
        n /= 1000


def hdur(s):
    s = int(s)
    if s < 60:
        return f'{s}s'
    if s < 3600:
        return f'{s // 60}m{s % 60:02d}s'
    return f'{s // 3600}h{(s % 3600) // 60:02d}m'


def show(b):
    """bytes path -> printable str (non-UTF-8 bytes become \\xNN; tabs/newlines escaped)."""
    s = b.decode('utf-8', 'backslashreplace')
    return s.replace('\t', '\\t').replace('\n', '\\n')


def rj(d, n):
    return d + b'/' + n if d else n


def alloc(size):
    return -(-size // BLOCK) * BLOCK


def fmt_time(ns):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ns / 1e9))


def norm_root(p):
    p = os.path.realpath(os.fsencode(p))
    return p.rstrip(b'/') or b'/'


def absp(root, rel):
    return root + b'/' + rel if root != b'/' else b'/' + rel


def hasher():
    return hashlib.blake2b(digest_size=16)


def open_ro(path):
    """Open read-only without updating atime when allowed (keeps sources untouched)."""
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOATIME', 0))
    except PermissionError:
        fd = os.open(path, os.O_RDONLY)
    return open(fd, 'rb', buffering=0)


def full_hash(path, size=None):
    h = hasher()
    buf = bytearray(BUF)
    mv = memoryview(buf)
    with open_ro(path) as f:
        while True:
            if STOP.is_set():
                raise Stopped
            n = f.readinto(buf)
            if not n:
                return h.digest()
            h.update(mv[:n])


def _phash(size, head, tail):
    h = hashlib.blake2b(digest_size=16, person=b'dedup-partial')
    h.update(size.to_bytes(8, 'little'))
    h.update(head)
    h.update(tail)
    return h.digest()


def partial_hash(path, size):
    """-> (phash, full hash or None). phash covers the size plus the first and last CHUNK bytes;
    files <= 2*CHUNK are hashed whole (phash == hash). Files <= FULL_READ_MAX are read once in full
    and return both hashes, so they never need a second read."""
    if size <= 2 * CHUNK:
        h = full_hash(path)
        return h, h
    if size <= FULL_READ_MAX:
        with open_ro(path) as f:
            data = f.readall()
        h = hasher()
        h.update(data)
        return _phash(size, data[:CHUNK], data[size - CHUNK:size]), h.digest()
    with open_ro(path) as f:
        fd = f.fileno()
        head = os.pread(fd, CHUNK, 0)
        tail = os.pread(fd, CHUNK, size - CHUNK)
    return _phash(size, head, tail), None


def hash_many(items, func, jobs):
    """items: tuples starting with (abs_path, size). Yields (item, result | OSError)."""
    def work(it):
        if STOP.is_set():
            raise Stopped
        try:
            return it, func(it[0], it[1])
        except OSError as e:
            return it, e
    if jobs <= 1:
        for it in items:
            yield work(it)
        return
    with ThreadPoolExecutor(jobs) as ex:
        for i in range(0, len(items), 4096):
            yield from ex.map(work, items[i:i + 4096])


class Progress:
    def __init__(self, what, total_files=None, total_bytes=None, reads=True):
        self.what, self.tf, self.tb, self.reads = what, total_files, total_bytes, reads
        self.f = self.b = 0
        self.t0 = self.last = time.monotonic()
        self.tty = sys.stderr.isatty()

    def add(self, files=1, nbytes=0):
        self.f += files
        self.b += nbytes
        now = time.monotonic()
        if now - self.last >= (0.5 if self.tty else 30):
            self.last = now
            self._print(now, False)

    def text(self, now, final):
        el = max(now - self.t0, 1e-6)
        parts = [f'{self.what}:', f'{self.f:,}' + (f'/{self.tf:,}' if self.tf else '') + ' files']
        if self.b or self.tb:
            parts.append(hsize(self.b) + (f'/{hsize(self.tb)}' if self.tb else ''))
            if self.reads:
                parts.append(f'{hsize(self.b / el)}/s')
        if not self.reads:
            parts.append(f'{self.f / el:,.0f} files/s')
        if final:
            parts.append(f'in {hdur(el)}')
        elif self.tb and self.b:
            parts.append('ETA ' + hdur((self.tb - self.b) / (self.b / el)))
        elif self.tf and self.f and self.tf > self.f:
            parts.append('ETA ' + hdur((self.tf - self.f) / (self.f / el)))
        return ' '.join(parts)

    def _print(self, now, final):
        status_line(self.tty, self.text(now, final), final)

    def done(self):
        self._print(time.monotonic(), True)


def status_line(tty, line, final):
    if tty:
        if not final:  # an overlong line would wrap and break the in-place redraw
            line = line[:shutil.get_terminal_size().columns - 1]
        sys.stderr.write('\r\x1b[K' + line + ('\n' if final else ''))
    else:
        sys.stderr.write(line + '\n')
    sys.stderr.flush()


class MultiProgress:
    """One status line for several Progress counters running at the same time."""
    def __init__(self, subs):
        self.subs = subs
        self.end = [None] * len(subs)
        self.tty = sys.stderr.isatty()
        self.last = time.monotonic()

    def add(self, i, files, nbytes):
        self.subs[i].f += files
        self.subs[i].b += nbytes
        now = time.monotonic()
        if now - self.last >= (0.5 if self.tty else 30):
            self.last = now
            parts = []
            for s, e in zip(self.subs, self.end):
                name = s.what.rsplit(' ', 1)[-1]
                if e is not None:
                    parts.append(f'{name} done')
                    continue
                el = max(now - s.t0, 1e-6)
                eta = f' ETA {hdur((s.tb - s.b) / (s.b / el))}' if s.b and s.tb else ''
                pct = f'{100 * s.b / s.tb:.1f}%' if s.tb else hsize(s.b)
                parts.append(f'{name} {pct} {hsize(s.b / el)}/s{eta}')
            status_line(self.tty, self.subs[0].what.rsplit(' ', 1)[0] + ': ' + ' · '.join(parts), False)

    def finish(self, i):
        self.end[i] = time.monotonic()

    def done(self):
        if self.tty:
            sys.stderr.write('\r\x1b[K')
        for s, e in zip(self.subs, self.end):
            status_line(False, s.text(e or time.monotonic(), True), True)


class step:
    """Context manager: prints 'what …' and the duration, for silent database work."""
    def __init__(self, what):
        self.what = what

    def __enter__(self):
        self.t0 = time.monotonic()
        sys.stderr.write(f'{self.what} …')
        sys.stderr.flush()

    def __exit__(self, exc, *_):
        sys.stderr.write(f' {hdur(time.monotonic() - self.t0)}\n' if exc is None else '\n')


def prefetch_db(db):
    """Read the database files sequentially once, so the random lookups that follow hit RAM
    instead of seeking on a spinning disk. Skipped if they don't fit comfortably in memory."""
    paths = []
    for _, _, f in db.execute('PRAGMA database_list'):
        for p in (f, f + '-wal') if f else ():
            if os.path.exists(p):
                paths.append(p)
    total = sum(os.path.getsize(p) for p in paths)
    try:
        with open('/proc/meminfo') as m:
            avail = next(int(l.split()[1]) * 1024 for l in m if l.startswith('MemAvailable:'))
    except (OSError, StopIteration):
        return
    if total > avail // 2:
        return
    prog = Progress('loading databases into memory', len(paths), total)
    buf = bytearray(8 * BUF)
    for p in paths:
        with open(p, 'rb', buffering=0) as f:
            while n := f.readinto(buf):
                if STOP.is_set():
                    raise Stopped
                prog.add(0, n)
        prog.add(1)
    prog.done()


def walk(root, excludes, prune=()):
    """Yields ('dir', rel, [(name, stat)]) per directory and (kind, relpath, reason) for
    'excluded' / 'skipped' (symlinks, special files) / 'error'. Never follows symlinks."""
    pats = [os.fsencode(p) for p in excludes]
    prune = set(prune)

    def excluded(rel, name):
        return any(fnmatch.fnmatchcase(name, p) or fnmatch.fnmatchcase(rel, p) for p in pats)

    stack = [b'']
    while stack:
        rel = stack.pop()
        absd = absp(root, rel) if rel else root
        try:
            with os.scandir(absd) as it:
                entries = list(it)
        except OSError as e:
            yield 'error', rel or b'.', f'cannot list directory: {e.strerror}'
            continue
        files, subdirs = [], []
        for e in entries:
            relp = rj(rel, e.name)
            if excluded(relp, e.name):
                yield 'excluded', relp, 'excluded by pattern'
                continue
            try:
                if e.is_symlink():
                    yield 'skipped', relp, 'symlink'
                elif e.is_dir(follow_symlinks=False):
                    if absp(root, relp) not in prune:
                        subdirs.append(relp)
                elif e.is_file(follow_symlinks=False):
                    files.append((e.name, e.stat(follow_symlinks=False)))
                else:
                    yield 'skipped', relp, 'special file'
            except OSError as ex:
                yield 'error', relp, ex.strerror
        yield 'dir', rel, files
        stack.extend(sorted(subdirs, reverse=True))


# ----------------------------------------------------------------------------- databases

INDEX_SCHEMA = '''
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS dirs(id INTEGER PRIMARY KEY, path BLOB UNIQUE NOT NULL, gen INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS files(
    id INTEGER PRIMARY KEY, dir INTEGER NOT NULL, name BLOB NOT NULL, lname BLOB NOT NULL,
    size INTEGER NOT NULL, mtime INTEGER NOT NULL, dev INTEGER, ino INTEGER,
    phash BLOB, hash BLOB, gen INTEGER NOT NULL, UNIQUE(dir, name));
CREATE INDEX IF NOT EXISTS files_size ON files(size);
CREATE INDEX IF NOT EXISTS files_hash ON files(hash);
CREATE INDEX IF NOT EXISTS files_ino ON files(dev, ino);
CREATE INDEX IF NOT EXISTS files_lname ON files(lname);
'''

PLAN_SCHEMA = '''
CREATE TABLE p.meta(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE p.sdirs(id INTEGER PRIMARY KEY, path BLOB UNIQUE NOT NULL);
CREATE TABLE p.src(
    id INTEGER PRIMARY KEY, dir INTEGER NOT NULL, name BLOB NOT NULL,
    size INTEGER NOT NULL, mtime INTEGER NOT NULL, dev INTEGER, ino INTEGER,
    phash BLOB, hash BLOB, err TEXT, cat TEXT, rep INTEGER, tpath BLOB, tmtime INTEGER,
    status TEXT, dest BLOB, forced TEXT);
CREATE TABLE p.skipped(path BLOB, kind TEXT, reason TEXT);
CREATE TABLE p.whole(path BLOB, files INTEGER, bytes INTEGER);
'''
PLAN_INDEXES = '''
CREATE INDEX p.src_size ON src(size);
CREATE INDEX p.src_hash ON src(hash);
CREATE INDEX p.src_dir ON src(dir);
CREATE INDEX p.src_cat ON src(cat);
'''

CACHE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS c.cache(path BLOB PRIMARY KEY, size INTEGER, mtime INTEGER,
                                   phash BLOB, hash BLOB) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS c.meta(k TEXT PRIMARY KEY, v TEXT);
'''


class Store:
    """TARGET/.dedup: index.sqlite (target index) + plans/<label>.{plan,cache,check}.sqlite"""

    def __init__(self, target):
        self.target = target
        self.dir = target + b'/' + DEDUP_DIR
        self.plans = self.dir + b'/plans'
        os.makedirs(self.plans, exist_ok=True)
        self._lock = open(self.dir + b'/lock', 'w')
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            die('another dedup run is using this target')
        self.db = sqlite3.connect(os.fsdecode(self.dir + b'/index.sqlite'), isolation_level=None)
        RUN['store'] = self
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=NORMAL')
        self.db.execute('PRAGMA cache_size=-262144')
        self.db.executescript(INDEX_SCHEMA)

    def meta(self, k, default=None, schema='main'):
        r = self.db.execute(f'SELECT v FROM {schema}.meta WHERE k=?', (k,)).fetchone()
        return r[0] if r else default

    def set_meta(self, k, v, schema='main'):
        self.db.execute(f'INSERT OR REPLACE INTO {schema}.meta VALUES(?,?)', (k, str(v)))

    def plan_path(self, label, kind):
        return self.plans + b'/' + label + b'.' + kind.encode() + b'.sqlite'

    def attach(self, path, name):
        self.db.execute('ATTACH DATABASE ? AS ' + name, (os.fsdecode(path),))

    def new_plan(self, label, kind):
        path = self.plan_path(label, kind)
        for suffix in (b'', b'-wal', b'-shm'):
            if os.path.exists(path + suffix):
                os.unlink(path + suffix)
        self.attach(path, 'p')
        self.db.executescript(PLAN_SCHEMA)

    def attach_cache(self, label, source):
        """The cache holds source hashes by (path, size, mtime); it is only trusted for the same
        source folder on the same filesystem (UUID). Otherwise -- another drive with the same label,
        or the identity can't be determined -- it is emptied."""
        self.attach(self.plan_path(label, 'cache'), 'c')
        self.db.executescript(CACHE_SCHEMA)
        ident = source_identity(source)
        stored = self.meta('source', None, 'c')
        if stored is None and ident is not None:
            # cache from a version that didn't record its source yet: adopt it
            self.set_meta('source', ident, 'c')
            self.db.commit()
        elif ident is None or stored != ident:
            self.db.execute('BEGIN')
            n = self.db.execute('DELETE FROM c.cache').rowcount
            self.set_meta('source', ident or '', 'c')
            self.db.execute('COMMIT')
            if n:
                print(f'source hash cache for label {show(label)} belongs to another source '
                      f'(or this one can\'t be identified): discarded {n:,} cached hashes', file=sys.stderr)

    def free_bytes(self):
        s = os.statvfs(self.target)
        return s.f_bavail * s.f_frsize


class Tx:
    """Batched transaction: commit() every few seconds so interrupted runs keep progress."""

    def __init__(self, db, every=5.0):
        self.db, self.every = db, every
        self.t = time.monotonic()
        db.execute('BEGIN')

    def tick(self):
        if time.monotonic() - self.t > self.every:
            self.flush()

    def flush(self):
        self.db.execute('COMMIT')
        self.db.execute('BEGIN')
        self.t = time.monotonic()

    def close(self):
        self.db.execute('COMMIT')


# ----------------------------------------------------------------------------- target index

def refresh_index(st, rehash_all=False):
    db, target = st.db, st.target
    gen = int(st.meta('gen', '0')) + 1
    excludes = json.loads(st.meta('excludes', '[]'))
    c = Counter()
    errors = []
    prog = Progress('indexing target', reads=False)
    db.execute('BEGIN')
    if rehash_all:
        db.execute('UPDATE files SET phash=NULL, hash=NULL')
    dirids = dict(db.execute('SELECT path, id FROM dirs'))
    for kind, rel, data in walk(target, excludes, prune={st.dir}):
        if kind != 'dir':
            c[kind] += 1
            if kind == 'error':
                errors.append((rel, data))
            continue
        did = dirids.get(rel)
        if did is None:
            did = db.execute('INSERT INTO dirs(path, gen) VALUES(?,?)', (rel, gen)).lastrowid
            dirids[rel] = did
            existing = {}
        else:
            db.execute('UPDATE dirs SET gen=? WHERE id=?', (gen, did))
            existing = {n: (i, s, m) for i, n, s, m in
                        db.execute('SELECT id, name, size, mtime FROM files WHERE dir=?', (did,))}
        for name, s in data:
            if name.endswith(PARTIAL_SUFFIX):
                continue
            c['files'] += 1
            c['bytes'] += s.st_size
            prog.add(1, s.st_size)
            ex = existing.get(name)
            if ex and ex[1] == s.st_size and ex[2] == s.st_mtime_ns:
                db.execute('UPDATE files SET gen=?, dev=?, ino=? WHERE id=?',
                           (gen, s.st_dev, s.st_ino, ex[0]))
            elif ex:
                c['changed'] += 1
                db.execute('UPDATE files SET gen=?, size=?, mtime=?, dev=?, ino=?, phash=NULL, hash=NULL '
                           'WHERE id=?', (gen, s.st_size, s.st_mtime_ns, s.st_dev, s.st_ino, ex[0]))
            else:
                # moved/renamed inside the target keeps inode+size+mtime: reuse its hashes
                old = db.execute('SELECT phash, hash FROM files WHERE dev=? AND ino=? AND size=? AND mtime=? '
                                 'AND phash IS NOT NULL LIMIT 1',
                                 (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns)).fetchone()
                c['moved' if old else 'added'] += 1
                db.execute('INSERT INTO files(dir, name, lname, size, mtime, dev, ino, phash, hash, gen) '
                           'VALUES(?,?,?,?,?,?,?,?,?,?)',
                           (did, name, name.lower(), s.st_size, s.st_mtime_ns, s.st_dev, s.st_ino,
                            old[0] if old else None, old[1] if old else None, gen))
    c['removed'] = db.execute('DELETE FROM files WHERE gen<>?', (gen,)).rowcount
    db.execute('DELETE FROM dirs WHERE gen<>?', (gen,))
    st.set_meta('gen', gen)
    st.set_meta('refreshed', time.strftime('%Y-%m-%d %H:%M:%S'))
    db.execute('COMMIT')
    prog.done()
    msg = (f'target index: {c["files"]:,} files, {hsize(c["bytes"])}  '
           f'(+{c["added"]:,} new, {c["changed"]:,} changed, {c["moved"]:,} moved, {c["removed"]:,} removed)')
    if c['error']:
        msg += f'\n  WARNING: {c["error"]} unreadable entries on target (treated as absent), e.g. ' + \
               ', '.join(show(p) for p, _ in errors[:3])
    if c['skipped']:
        msg += f'\n  {c["skipped"]} symlinks/special files ignored'
    print(msg, file=sys.stderr)
    return c


def ensure_index(st, args):
    """Refresh the target index unless --no-reindex. Returns None if refreshed, else the time of
    the last refresh (the index may be stale)."""
    last = st.meta('refreshed')
    if not getattr(args, 'no_reindex', False):
        refresh_index(st)
        RUN['reindexed'] = True
        return None
    if last is None:
        print('no target index yet — building it despite --no-reindex', file=sys.stderr)
        refresh_index(st)
        return None
    print(f'WARNING: --no-reindex: using the target index from {last}. Changes on the target since then\n'
          f'         (deleted, moved or edited files) are not seen. Files deleted since then still count\n'
          f'         as present, so their source copies would NOT be imported.', file=sys.stderr)
    return last


def hash_target_all(st, jobs):
    db = st.db
    rows = db.execute('SELECT f.id, d.path, f.name, f.size FROM files f JOIN dirs d ON d.id=f.dir '
                      'WHERE f.hash IS NULL ORDER BY f.dev, f.ino').fetchall()
    items = [(absp(st.target, rj(d, n)), size, fid) for fid, d, n, size in rows]

    def both(path, size):
        ph, h = partial_hash(path, size)
        return ph, h or full_hash(path)
    tx = Tx(db)
    prog = Progress('hashing target', len(items), sum(i[1] for i in items))
    bad = 0
    for it, res in hash_many(items, both, jobs):
        prog.add(1, it[1])
        if isinstance(res, OSError):
            bad += 1
        else:
            db.execute('UPDATE files SET phash=?, hash=? WHERE id=?', (res[0], res[1], it[2]))
        tx.tick()
    tx.close()
    prog.done()
    if bad:
        print(f'  {bad} target files unreadable', file=sys.stderr)


# ----------------------------------------------------------------------------- source scan + classify

def scan_source(st, source, excludes, prune, events=None, what='scanning source', expect=None):
    """Fill p.sdirs / p.src / p.skipped from a walk of the source (or from a saved scan's events).
    expect: file count of the previous scan, used only for the ETA."""
    db = st.db
    prog = Progress(what, expect, reads=False)
    db.execute('BEGIN')
    for kind, rel, data in (walk(source, excludes, prune) if events is None else events):
        if kind != 'dir':
            db.execute('INSERT INTO p.skipped VALUES(?,?,?)', (rel, kind, data))
            continue
        did = db.execute('INSERT INTO p.sdirs(path) VALUES(?)', (rel,)).lastrowid
        rows = []
        for name, s in data:
            cached = db.execute('SELECT phash, hash FROM c.cache WHERE path=? AND size=? AND mtime=?',
                                (rj(rel, name), s.st_size, s.st_mtime_ns)).fetchone() or (None, None)
            rows.append((did, name, s.st_size, s.st_mtime_ns, s.st_dev, s.st_ino, *cached))
            prog.add(1, s.st_size)
        db.executemany('INSERT INTO p.src(dir, name, size, mtime, dev, ino, phash, hash) '
                       'VALUES(?,?,?,?,?,?,?,?)', rows)
    db.execute('COMMIT')
    db.executescript(PLAN_INDEXES)
    prog.done()


SavedStat = namedtuple('SavedStat', 'st_size st_mtime_ns st_dev st_ino')
SCAN_SCHEMA = '''
CREATE TABLE s.meta(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE s.sdirs(id INTEGER PRIMARY KEY, path BLOB NOT NULL);
CREATE TABLE s.files(dir INTEGER, name BLOB, size INTEGER, mtime INTEGER, dev INTEGER, ino INTEGER);
CREATE TABLE s.skipped(path BLOB, kind TEXT, reason TEXT);
'''


def save_scan(st, label, source, excludes):
    """Copy the fresh source scan from the plan to plans/<label>.scan.sqlite, so a re-run of compare
    (e.g. after Ctrl-C) doesn't have to walk the source again. Written to a temp file, then renamed."""
    path = st.plan_path(label, 'scan')
    tmp = path + b'.tmp'
    for f in (tmp, tmp + b'-journal'):
        if os.path.exists(f):
            os.unlink(f)
    st.attach(tmp, 's')
    db = st.db
    db.executescript(SCAN_SCHEMA)
    db.execute('BEGIN')
    db.execute('INSERT INTO s.sdirs SELECT id, path FROM p.sdirs')
    db.execute('INSERT INTO s.files SELECT dir, name, size, mtime, dev, ino FROM p.src')
    db.execute('INSERT INTO s.skipped SELECT path, kind, reason FROM p.skipped')
    for k, v in (('source', os.fsdecode(source)), ('excludes', json.dumps(excludes)),
                 ('scanned', time.time())):
        st.set_meta(k, v, 's')
    db.execute('COMMIT')
    db.execute('CREATE INDEX s.files_dir ON files(dir)')
    db.execute('DETACH s')
    os.replace(tmp, path)


def previous_scan_count(st, label, source):
    """Number of files the last complete scan of this source found (for an ETA), or None."""
    for kind, table in (('scan', 'files'), ('plan', 'src')):
        path = st.plan_path(label, kind)
        if not os.path.exists(path):
            continue
        st.attach(path, 'x')
        try:
            if (os.fsencode(st.meta('source', '', 'x')) == source
                    and (kind == 'scan' or st.meta('complete', '0', 'x') == '1')):
                return st.db.execute(f'SELECT count(*) FROM x.{table}').fetchone()[0]
        except sqlite3.DatabaseError:
            pass
        finally:
            st.db.execute('DETACH x')
    return None


def attach_saved_scan(st, label, source, excludes):
    """Attach plans/<label>.scan.sqlite as 's' if it was made for this source with these excludes.
    Returns its scan time (epoch seconds) or None."""
    path = st.plan_path(label, 'scan')
    if not os.path.exists(path):
        return None
    st.attach(path, 's')
    try:
        if (os.fsencode(st.meta('source', '', 's')) == source
                and json.loads(st.meta('excludes', 'null', 's')) == excludes):
            return float(st.meta('scanned', None, 's'))
    except (sqlite3.DatabaseError, ValueError, TypeError):
        pass
    st.db.execute('DETACH s')
    return None


def saved_events(db):
    """Replay a saved scan in the same shape as walk()."""
    for rel, kind, reason in db.execute('SELECT path, kind, reason FROM s.skipped').fetchall():
        yield kind, rel, reason
    cur, rel_cur, files = None, None, []
    for did, rel, name, size, mtime, dev, ino in db.execute(
            'SELECT d.id, d.path, f.name, f.size, f.mtime, f.dev, f.ino '
            'FROM s.sdirs d LEFT JOIN s.files f ON f.dir = d.id ORDER BY d.id'):
        if did != cur:
            if cur is not None:
                yield 'dir', rel_cur, files
            cur, rel_cur, files = did, rel, []
        if name is not None:
            files.append((name, SavedStat(size, mtime, dev, ino)))
    if cur is not None:
        yield 'dir', rel_cur, files


def classify(st, source, jobs, mode='compare', label=None, whole=(), source_jobs=1):
    """Fill p.src.cat: present / dup / new / older / newer / error. Returns unreadable target files.
    mode='compare' also forces empty files and whole folders (p.src.forced); mode='check' only
    asks whether the content is on the target (empty files: an empty file of the same name)."""
    db, target = st.db, st.target
    terr = []
    par = separate_disks(target, source)
    prefetch_db(db)

    # sizes worth hashing: shared with the target or with another source file
    with step('matching file sizes against the target'):
        db.executescript('''
          DROP TABLE IF EXISTS temp.csz;
          CREATE TEMP TABLE csz(size INTEGER PRIMARY KEY);
          INSERT OR IGNORE INTO csz SELECT size FROM p.src WHERE err IS NULL GROUP BY size HAVING count(*) > 1;
          INSERT OR IGNORE INTO csz SELECT DISTINCT s.size FROM p.src s
              WHERE s.err IS NULL AND EXISTS (SELECT 1 FROM main.files f WHERE f.size = s.size);
        ''')

    # --- stage 1: partial hashes
    with step('listing files to hash'):
        t_items = [(absp(target, rj(d, n)), size, fid) for fid, d, n, size in db.execute(
            'SELECT f.id, d.path, f.name, f.size FROM main.files f JOIN main.dirs d ON d.id=f.dir '
            'WHERE f.phash IS NULL AND f.size IN (SELECT size FROM csz) ORDER BY f.dev, f.ino')]
        s_items = [(absp(source, rj(d, n)), size, sid, rj(d, n), mt) for sid, d, n, size, mt in db.execute(
            'SELECT s.id, d.path, s.name, s.size, s.mtime FROM p.src s JOIN p.sdirs d ON d.id=s.dir '
            'WHERE s.phash IS NULL AND s.err IS NULL AND s.size IN (SELECT size FROM csz) ORDER BY s.ino')]
    _hash_phases(st, 'partial hash', [('target', t_items, jobs, _store_t_partial, terr),
                                      ('source', s_items, source_jobs, _store_s_partial, None)],
                 partial_hash, par, partial=True)

    # --- stage 2: full hashes for (size, phash) groups with >= 2 members incl. >= 1 source file
    with step('finding candidates for full hashing'):
        db.executescript('''
            DROP TABLE IF EXISTS temp.cgrp;
            CREATE TEMP TABLE cgrp(size INTEGER, phash BLOB, PRIMARY KEY(size, phash));
            INSERT INTO cgrp SELECT size, phash FROM (
                SELECT size, phash, 1 AS s FROM p.src WHERE err IS NULL AND phash IS NOT NULL
                UNION ALL
                SELECT size, phash, 0 FROM main.files
                    WHERE phash IS NOT NULL AND size IN (SELECT size FROM csz))
            GROUP BY size, phash HAVING count(*) > 1 AND sum(s) > 0;
        ''')
        # target: one unhashed file per group, and only where no target file of the group has a full
        # hash yet; further copies are read only if that one doesn't match (round 2 below)
        t_items = [(absp(target, rj(d, n)), size, fid) for fid, d, n, size in db.execute(
            'SELECT f.id, d.path, f.name, f.size FROM main.files f JOIN main.dirs d ON d.id=f.dir '
            'WHERE f.id IN (SELECT min(f1.id) FROM cgrp g JOIN main.files f1 '
            '               ON f1.size=g.size AND f1.phash=g.phash AND f1.hash IS NULL '
            '               WHERE NOT EXISTS (SELECT 1 FROM main.files f2 WHERE f2.size=g.size '
            '                                 AND f2.phash=g.phash AND f2.hash IS NOT NULL) '
            '               GROUP BY g.size, g.phash) '
            'ORDER BY f.dev, f.ino')]
        s_items = [(absp(source, rj(d, n)), size, sid, rj(d, n), mt) for sid, d, n, size, mt in db.execute(
            'SELECT s.id, d.path, s.name, s.size, s.mtime FROM p.src s '
            'JOIN cgrp g ON s.size=g.size AND s.phash=g.phash JOIN p.sdirs d ON d.id=s.dir '
            'WHERE s.hash IS NULL AND s.err IS NULL ORDER BY s.ino')]
    _hash_phases(st, 'full hash', [('target', t_items, jobs, _store_t_full, terr),
                                   ('source', s_items, source_jobs, _store_s_full, None)],
                 full_hash, par)

    # round 2: groups where a source hash has no match on the target yet -> the remaining target copies
    with step('checking which source files still lack a match'):
        tried = {it[2] for it in t_items}
        t_items = [(absp(target, rj(d, n)), size, fid) for fid, d, n, size in db.execute(
            'SELECT f.id, d.path, f.name, f.size FROM main.files f JOIN main.dirs d ON d.id=f.dir '
            'JOIN (SELECT DISTINCT s.size, s.phash FROM p.src s JOIN cgrp g ON s.size=g.size AND s.phash=g.phash '
            '      WHERE s.err IS NULL AND s.hash IS NOT NULL '
            '      AND NOT EXISTS (SELECT 1 FROM main.files f3 WHERE f3.hash=s.hash)) u '
            'ON f.size=u.size AND f.phash=u.phash '
            'WHERE f.hash IS NULL ORDER BY f.dev, f.ino') if fid not in tried]
    _hash_phases(st, 'full hash', [('target (other copies)', t_items, jobs, _store_t_full, terr)],
                 full_hash, False)

    # --- stage 3: categories
    with step('sorting files into categories'):
        db.executescript('''
            BEGIN;
            UPDATE p.src SET cat=NULL, rep=NULL, tpath=NULL, tmtime=NULL;
            UPDATE p.src SET cat='error' WHERE err IS NOT NULL;
            UPDATE p.src SET cat='present' WHERE cat IS NULL AND hash IS NOT NULL
                AND EXISTS (SELECT 1 FROM main.files f WHERE f.hash = src.hash);

            -- several source copies of the same new content: copy one. Prefer the copy whose
            -- folder is "most new" (keeps folders complete), then the oldest folder.
            DROP TABLE IF EXISTS temp.dr;
            CREATE TEMP TABLE dr AS SELECT dir, avg(cat IS NULL) AS r, min(mtime) AS mt
                FROM p.src GROUP BY dir;
            DROP TABLE IF EXISTS temp.rk;
            CREATE TEMP TABLE rk AS
                SELECT s.id AS id,
                       row_number() OVER w AS rn,
                       first_value(s.id) OVER w AS rep
                FROM p.src s JOIN dr ON dr.dir=s.dir JOIN p.sdirs d ON d.id=s.dir
                WHERE s.cat IS NULL AND s.hash IS NOT NULL
                WINDOW w AS (PARTITION BY s.hash ORDER BY dr.r DESC, dr.mt, d.path, s.mtime, s.id);
            CREATE INDEX temp.rk_id ON rk(id);
            UPDATE p.src SET cat='dup', rep=(SELECT rep FROM rk WHERE rk.id=src.id)
                WHERE id IN (SELECT id FROM rk WHERE rn > 1);
            UPDATE p.src SET cat='new' WHERE cat IS NULL;
            COMMIT;
        ''')

    # new files that were never opened (unique size): make sure they are readable at all
    db.execute('BEGIN')
    for sid, d, n in db.execute("SELECT s.id, d.path, s.name FROM p.src s JOIN p.sdirs d ON d.id=s.dir "
                                "WHERE s.cat='new' AND s.hash IS NULL AND s.phash IS NULL").fetchall():
        if not os.access(absp(source, rj(d, n)), os.R_OK):
            db.execute("UPDATE p.src SET cat='error', err='permission denied' WHERE id=?", (sid,))
    db.execute('COMMIT')

    # empty files have no content, only a name (markers like __init__.py, .gitkeep, .nomedia):
    # compare always copies them (unless already at their import destination)
    db.execute('BEGIN')
    for sid, d, n in db.execute("SELECT s.id, d.path, s.name FROM p.src s JOIN p.sdirs d ON d.id=s.dir "
                                "WHERE s.size=0 AND s.cat<>'error'").fetchall():
        if mode == 'check':
            ok = db.execute('SELECT 1 FROM main.files WHERE size=0 AND lname=? LIMIT 1', (n.lower(),)).fetchone()
            db.execute('UPDATE p.src SET cat=?, rep=NULL WHERE id=?', ('present' if ok else 'new', sid))
        else:
            ok = db.execute('SELECT 1 FROM main.files f JOIN main.dirs dd ON dd.id=f.dir '
                            'WHERE dd.path=? AND f.name=? AND f.size=0', (dest_rel(label, 'new', d), n)).fetchone()
            db.execute('UPDATE p.src SET cat=?, forced=?, rep=NULL WHERE id=?',
                       ('present', None, sid) if ok else ('new', 'empty', sid))
    db.execute('COMMIT')

    # older / newer versions: same file name in a same-named folder on the target (unique match)
    with step('looking for older/newer versions'):
        db.execute('BEGIN')
        cache = {}
        rows = db.execute("SELECT s.id, s.name, s.mtime, d.path FROM p.src s JOIN p.sdirs d ON d.id=s.dir "
                          "WHERE s.cat='new' AND s.forced IS NULL AND s.size > 0 AND d.path <> x''").fetchall()
        for sid, name, mtime, dpath in rows:
            lname = name.lower()
            cands = cache.get(lname)
            if cands is None:
                if len(cache) > 200_000:
                    cache.clear()
                cands = cache[lname] = [
                    (tdir.rsplit(b'/', 1)[-1].lower(), tm, rj(tdir, tname)) for tm, tdir, tname in db.execute(
                        'SELECT f.mtime, d.path, f.name FROM main.files f JOIN main.dirs d ON d.id=f.dir '
                        'WHERE f.lname=?', (lname,))]
            parent = dpath.rsplit(b'/', 1)[-1].lower()
            match = [(tm, tp) for pl, tm, tp in cands if pl == parent]
            if len(match) == 1:
                tm, tp = match[0]
                cat = 'older' if tm > mtime + MTIME_TOL_NS else 'newer'
                db.execute('UPDATE p.src SET cat=?, tpath=?, tmtime=? WHERE id=?', (cat, tp, tm, sid))
        db.execute('COMMIT')
    if mode == 'compare':
        with step('checking folders to keep whole'):
            keep_whole(db, whole)
    return terr


def whole_roots(db, whole):
    """Folders that only work complete: contain a .git entry (if '.git' in whole) or match a pattern."""
    pats = [os.fsencode(p) for p in whole if p != '.git']
    cands = set()
    paths = [p for (p,) in db.execute('SELECT path FROM p.sdirs')]
    parent = lambda p: p.rsplit(b'/', 1)[0] if b'/' in p else b''
    if '.git' in whole:
        cands.update(parent(p) for p in paths if p.rsplit(b'/', 1)[-1] == b'.git')
        cands.update(d for (d,) in db.execute("SELECT d.path FROM p.src s JOIN p.sdirs d ON d.id=s.dir "
                                              "WHERE s.name=x'2e676974'"))  # '.git' file (worktree)
    for p in paths:
        if p and any(fnmatch.fnmatchcase(p.rsplit(b'/', 1)[-1], x) or fnmatch.fnmatchcase(p, x) for x in pats):
            cands.add(p)
    roots = []
    for r in sorted(cands, key=len):
        if not any(k == b'' or r == k or r.startswith(k + b'/') for k in roots):
            roots.append(r)
    return roots


def keep_whole(db, whole):
    """If anything in such a folder gets copied, copy all of it (into new/), duplicates included."""
    db.execute('BEGIN')
    for r in whole_roots(db, whole):
        if r:
            sub = '(SELECT id FROM p.sdirs WHERE path=? OR substr(path, 1, ?)=?)'
            arg = (r, len(r) + 1, r + b'/')
        else:
            sub, arg = '(SELECT id FROM p.sdirs)', ()
        # empty files don't count: they are always copied and can't be matched, so nearly every
        # repo would be copied whole just because of an empty __init__.py or .git/refs file
        q = (f"SELECT 1 FROM p.src WHERE dir IN {sub} AND cat IN ('new','older','newer') "
             f"AND forced IS NULL LIMIT 1")
        if not db.execute(q, arg).fetchone():
            continue  # nothing new in there: the folder already exists on the target
        n, b = db.execute(f"SELECT count(*), coalesce(sum(size),0) FROM p.src WHERE dir IN {sub} "
                          f"AND cat IN ('present','dup','older','newer')", arg).fetchone()
        db.execute(f"UPDATE p.src SET cat='new', forced='whole', rep=NULL, tpath=NULL, tmtime=NULL "
                   f"WHERE dir IN {sub} AND cat IN ('present','dup','older','newer')", arg)
        db.execute('INSERT INTO p.whole VALUES(?,?,?)', (r, n, b))
    db.execute('COMMIT')


def empty_dirs(db):
    """Source folders without any files below them, recreated (under new/) where their parent
    folder is being imported anyway."""
    paths = [p for (p,) in db.execute('SELECT path FROM p.sdirs')]
    parent = lambda p: p.rsplit(b'/', 1)[0] if b'/' in p else b''
    sub_f, sub_n = Counter(), Counter()
    for p, cat, n in db.execute('SELECT d.path, s.cat, count(*) FROM p.src s JOIN p.sdirs d ON d.id=s.dir '
                                'GROUP BY s.dir, s.cat'):
        sub_f[p] += n
        if cat == 'new':
            sub_n[p] += n
    for p in sorted(paths, key=lambda p: p.count(b'/'), reverse=True):
        if p:
            sub_f[parent(p)] += sub_f[p]
            sub_n[parent(p)] += sub_n[p]
    create = set()
    for p in sorted(paths, key=lambda p: (p.count(b'/'), p)):
        if p and not sub_f[p] and (parent(p) in create or sub_n[parent(p)] > 0):
            create.add(p)
    return sorted(create)


def _hash_phases(st, what, sides, func, parallel, partial=False):
    """sides: [(name, items, jobs, store, terr)]. With parallel=True (target and source on different
    disks) every side is read in its own thread at the same time; database writes stay in this thread."""
    sides = [sd for sd in sides if sd[1]]
    if not sides:
        return
    cost = (lambda s: s if s <= FULL_READ_MAX else 2 * CHUNK) if partial else (lambda s: s)
    progs = [Progress(f'{what} {name}', len(items), sum(cost(i[1]) for i in items))
             for name, items, *_ in sides]
    tx = Tx(st.db)

    def put(i, it, res):
        store, terr = sides[i][3], sides[i][4]
        if isinstance(res, OSError) and terr is not None:
            terr.append((it[0], res.strerror))
        else:
            store(st.db, it, res)
        tx.tick()

    try:
        _run_sides(sides, func, parallel, cost, progs, tx, put)
    except KeyboardInterrupt:
        tx.close()  # every stored hash is complete on its own: keep them, also when stopping
        raise
    tx.close()


def _run_sides(sides, func, parallel, cost, progs, tx, put):
    if not parallel or len(sides) == 1:
        for i, (name, items, jobs, *_) in enumerate(sides):
            for it, res in hash_many(items, func, jobs):
                progs[i].add(1, cost(it[1]))
                put(i, it, res)
            tx.flush()
            progs[i].done()
        return

    q = queue.Queue(maxsize=4096)

    def feed(i, items, jobs):
        try:
            for it, res in hash_many(items, func, jobs):
                q.put((i, it, res))
            q.put((i, None, None))
        except BaseException as e:  # Stopped (Ctrl-C) or a bug: hand it to the main thread
            q.put((i, None, e))

    mp = MultiProgress(progs)
    for i, (name, items, jobs, *_) in enumerate(sides):
        threading.Thread(target=feed, args=(i, items, jobs), daemon=True).start()
    running, failure = len(sides), None
    while running:
        try:
            i, it, res = q.get(timeout=0.5)
        except queue.Empty:
            continue
        if it is None:
            running -= 1
            mp.finish(i)
            failure = failure or res
            continue
        mp.add(i, 1, cost(it[1]))
        put(i, it, res)
    if failure is not None:
        raise failure
    mp.done()


def mount_of(path):
    """-> (mount point, folder of the filesystem mounted there, device) for the mount holding
    path, from /proc/self/mountinfo; (None, None, None) if unknown."""
    path = os.fsdecode(os.path.realpath(path))
    unesc = lambda x: re.sub(r'\\([0-7]{3})', lambda m: chr(int(m.group(1), 8)), x)
    best = (None, None, None)
    try:
        with open('/proc/self/mountinfo') as f:
            for line in f:
                pre, post = line.split(' - ', 1)
                fields = pre.split()
                mnt, root, src = unesc(fields[4]), unesc(fields[3]), unesc(post.split()[1])
                if ((path == mnt or path.startswith(mnt.rstrip('/') + '/'))
                        and len(mnt) >= len(best[0] or '')):
                    best = (mnt, root, src)
    except (OSError, ValueError, IndexError):
        return None, None, None
    return best


def source_identity(path):
    """'<filesystem UUID>:<path inside that filesystem>', or None if the UUID can't be found.
    Stays the same when a drive is mounted elsewhere; differs between two drives mounted
    one after the other at the same path."""
    mnt, root, dev = mount_of(path)
    if not dev or not dev.startswith('/dev/'):
        return None
    dev = os.path.realpath(dev)
    uuid = None
    try:
        for name in os.listdir('/dev/disk/by-uuid'):
            if os.path.realpath('/dev/disk/by-uuid/' + name) == dev:
                uuid = name
                break
    except OSError:
        return None
    if uuid is None:
        return None
    rel = os.path.relpath(os.fsdecode(os.path.realpath(path)), mnt)
    # btrfs: one UUID covers all subvolumes; mountinfo's root names the subvolume
    return f'{uuid}:{os.path.normpath(os.path.join(root, rel))}'


def disk_of(path):
    """Best effort: name of the physical disk holding path (e.g. 'sdb'), or None."""
    _, _, dev = mount_of(path)
    if not dev or not dev.startswith('/dev/'):
        return None
    name = os.path.basename(os.path.realpath(dev))
    sysp = '/sys/class/block/' + name
    if os.path.exists(sysp + '/partition'):
        return os.path.basename(os.path.dirname(os.path.realpath(sysp)))
    return name if os.path.exists(sysp) else None


def separate_disks(target, source):
    """True if target and source can be read at the same time without fighting over one disk."""
    dt, ds = disk_of(target), disk_of(source)
    if dt and ds:
        return dt != ds
    return os.stat(target).st_dev != os.stat(source).st_dev


def _store_t_partial(db, it, res):
    db.execute('UPDATE main.files SET phash=?, hash=coalesce(?, hash) WHERE id=?', (res[0], res[1], it[2]))


def _store_t_full(db, it, res):
    db.execute('UPDATE main.files SET hash=? WHERE id=?', (res, it[2]))


def _cache_put(db, rel, size, mtime, phash, h):
    db.execute('INSERT INTO c.cache VALUES(?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET '
               'size=excluded.size, mtime=excluded.mtime, phash=excluded.phash, hash=excluded.hash',
               (rel, size, mtime, phash, h))


def _store_s_partial(db, it, res):
    if isinstance(res, OSError):
        db.execute('UPDATE p.src SET err=? WHERE id=?', (res.strerror or str(res), it[2]))
        return
    db.execute('UPDATE p.src SET phash=?, hash=coalesce(?, hash) WHERE id=?', (res[0], res[1], it[2]))
    _cache_put(db, it[3], it[1], it[4], res[0], res[1])


def _store_s_full(db, it, res):
    if isinstance(res, OSError):
        db.execute('UPDATE p.src SET err=? WHERE id=?', (res.strerror or str(res), it[2]))
        return
    db.execute('UPDATE p.src SET hash=? WHERE id=?', (res, it[2]))
    db.execute('UPDATE c.cache SET hash=? WHERE path=?', (res, it[3]))


# ----------------------------------------------------------------------------- reporting

class Node:
    __slots__ = ('path', 'children', 'own', 'tot')

    def __init__(self, path):
        self.path, self.children, self.own, self.tot = path, [], Counter(), Counter()


FORCED_LABEL = {'empty': ' (empty file)', 'whole': ' (whole folder)'}


def cat_label(cat, forced=None):
    return CAT_LABEL[cat] + FORCED_LABEL.get(forced, '')


def build_tree(db):
    nodes = {b'': Node(b'')}
    for (path,) in db.execute('SELECT path FROM p.sdirs'):
        nodes.setdefault(path, Node(path))
    for path, cat, n, b in db.execute('SELECT d.path, s.cat, count(*), sum(s.size) FROM p.src s '
                                      'JOIN p.sdirs d ON d.id=s.dir GROUP BY s.dir, s.cat'):
        node = nodes[path]
        node.own[cat] += n
        node.own[cat + '_b'] += b
    for path in sorted(nodes, key=lambda p: -p.count(b'/') if p else 1):
        if path:
            parent = path.rsplit(b'/', 1)[0] if b'/' in path else b''
            nodes.setdefault(parent, Node(parent)).children.append(nodes[path])
    for path in sorted(nodes, key=lambda p: (p.count(b'/') + 1) if p else 0, reverse=True):
        n = nodes[path]
        n.tot.update(n.own)
        for ch in n.children:
            n.tot.update(ch.tot)
        n.children.sort(key=lambda x: x.path)
    return nodes[b'']


def node_counts(cnt):
    total = sum(cnt[c] for c in CAT_LABEL)
    copy = sum(cnt[c] for c in COPY_CATS)
    copy_b = sum(cnt[c + '_b'] for c in COPY_CATS)
    total_b = sum(cnt[c + '_b'] for c in CAT_LABEL)
    return total, copy, copy_b, total_b


def render_tree(root, max_depth, max_lines):
    out = []

    def line(node, depth):
        total, copy, copy_b, total_b = node_counts(node.tot)
        if total == 0:
            return
        name = (show(node.path) + '/') if node.path else './'
        err = f'  ⚠ {node.tot["error"]} unreadable' if node.tot['error'] else ''
        ind = '  ' * depth
        if copy == 0:
            extra = []
            if node.tot['present']:
                extra.append(f'{node.tot["present"]:,} on target')
            if node.tot['dup']:
                extra.append(f'{node.tot["dup"]:,} dup in source')
            out.append(f'{ind}✓ {name}  {total:,} files, {hsize(total_b)} ({", ".join(extra) or "-"}){err}')
        elif copy == total:
            out.append(f'{ind}✚ {name}  {total:,} file{"s" if total != 1 else ""}, {hsize(total_b)}{err}')
        else:
            out.append(f'{ind}◐ {name}  {copy:,} of {total:,} files to copy, '
                       f'{hsize(copy_b)} of {hsize(total_b)}{err}')
            if max_depth is None or depth < max_depth:
                own_total, own_copy, own_copy_b, _ = node_counts(node.own)
                if own_copy:
                    out.append(f'{ind}  · {own_copy:,} of {own_total:,} files directly in {name} '
                               f'to copy, {hsize(own_copy_b)}')
                for ch in node.children:
                    line(ch, depth + 1)

    line(root, 0)
    if max_lines and len(out) > max_lines:
        out = out[:max_lines] + [f'… {len(out) - max_lines} more lines in the full report']
    return out


def summary_lines(st, header):
    db = st.db
    rows = {cat: (n, b or 0) for cat, n, b in
            db.execute('SELECT cat, count(*), sum(size) FROM p.src GROUP BY cat')}
    get = lambda c: rows.get(c, (0, 0))
    total_n = sum(n for n, _ in rows.values())
    total_b = sum(b for _, b in rows.values())
    copy_n = sum(get(c)[0] for c in COPY_CATS)
    copy_b = sum(get(c)[1] for c in COPY_CATS)
    copy_alloc = sum(alloc(s) for (s,) in db.execute(
        f"SELECT size FROM p.src WHERE cat IN ({','.join('?' * len(COPY_CATS))})", COPY_CATS))
    out = list(header)
    out.append('')
    out.append(f'  {"":24}{"files":>12}{"size":>12}')
    out.append(f'  {"total":24}{total_n:>12,}{hsize(total_b):>12}')
    for c in ('present', 'dup'):
        out.append(f'  {CAT_LABEL[c]:24}{get(c)[0]:>12,}{hsize(get(c)[1]):>12}')
    out.append(f'  {"to copy":24}{copy_n:>12,}{hsize(copy_b):>12}')
    for c in COPY_CATS:
        out.append(f'    {CAT_LABEL[c]:22}{get(c)[0]:>12,}{hsize(get(c)[1]):>12}')
        if c == 'new':
            fw, fwb = db.execute("SELECT count(*), coalesce(sum(size),0) FROM p.src WHERE forced='whole'").fetchone()
            fe = db.execute("SELECT count(*) FROM p.src WHERE forced='empty'").fetchone()[0]
            if fw:
                nw = db.execute('SELECT count(*) FROM p.whole').fetchone()[0]
                out.append(f'      incl. {fw:,} files ({hsize(fwb)}) present elsewhere, copied to keep '
                           f'{nw:,} folders whole')
            if fe:
                out.append(f'      incl. {fe:,} empty files')
    label = os.fsencode(st.meta('label', '', 'p'))
    ed = sum(1 for d in empty_dirs(db) if not os.path.isdir(absp(st.target, dest_rel(label, 'new', d))))
    if ed:
        out.append(f'  {"empty folders recreated":24}{ed:>12,}')
    if get('error')[0]:
        out.append(f'  {"unreadable (skipped)":24}{get("error")[0]:>12,}{hsize(get("error")[1]):>12}')
    sk = Counter(k for (k,) in db.execute('SELECT kind FROM p.skipped'))
    if sk:
        out.append('  not scanned: ' + ', '.join(f'{v} {k}' for k, v in sorted(sk.items())))
    return out, copy_b, copy_alloc


def space_lines(st, copy_b, copy_alloc):
    free = st.free_bytes()
    need = needed_space(copy_alloc)
    out = ['', 'SPACE',
           f'  to copy {hsize(copy_b)}  (≈ {hsize(copy_alloc)} on disk incl. block rounding)',
           f'  free on target {hsize(free)}  →  ' +
           (f'OK, ≈ {hsize(free - copy_alloc)} left afterwards' if free >= need
            else f'NOT ENOUGH: need ≈ {hsize(need)} incl. margin, short by {hsize(need - free)}')]
    return out


def needed_space(copy_alloc):
    return copy_alloc + max(1_000_000_000, copy_alloc // 100) if copy_alloc else 0


def detail_lines(st, terr):
    db = st.db
    out = []
    rows = db.execute('SELECT path, files, bytes FROM p.whole ORDER BY path').fetchall()
    if rows:
        out += ['', 'KEPT WHOLE (git repos / --whole patterns): copied completely, incl. content present elsewhere']
        out += [f'  {show(p) or "./"}/  +{n:,} files, {hsize(b)} that would otherwise be skipped' for p, n, b in rows]
    for cat, title in (('older', 'OLDER VERSIONS (target has a newer file of the same name in a same-named folder)'),
                       ('newer', 'NEWER ON SOURCE (target has an older or same-age file of the same name '
                                 'in a same-named folder — review)')):
        rows = db.execute('SELECT d.path, s.name, s.mtime, s.tpath, s.tmtime FROM p.src s '
                          'JOIN p.sdirs d ON d.id=s.dir WHERE s.cat=? ORDER BY d.path, s.name', (cat,)).fetchall()
        if rows:
            out += ['', title]
            for d, n, m, tp, tm in rows:
                out.append(f'  {show(rj(d, n))}  [{fmt_time(m)}]  ↔  target: {show(tp)}  [{fmt_time(tm)}]')
    rows = db.execute("SELECT d.path, s.name, s.err FROM p.src s JOIN p.sdirs d ON d.id=s.dir "
                      "WHERE s.cat='error'").fetchall()
    rows += db.execute("SELECT path, NULL, reason FROM p.skipped WHERE kind='error'").fetchall()
    if rows:
        out += ['', 'UNREADABLE ON SOURCE (not copied)']
        out += [f'  {show(rj(d, n) if n else d)}: {e}' for d, n, e in rows]
    rows = db.execute("SELECT path, kind, reason FROM p.skipped WHERE kind<>'error' ORDER BY path").fetchall()
    if rows:
        out += ['', 'NOT SCANNED (symlinks, special files, excluded)']
        out += [f'  {show(p)}  ({r})' for p, _, r in rows]
    if terr:
        out += ['', 'UNREADABLE ON TARGET (treated as absent)']
        out += [f'  {show(p)}: {e}' for p, e in terr]
    return out


# ----------------------------------------------------------------------------- commands

def not_a_dir(what, p):
    hint = ''
    if b'\\' in p and os.path.isdir(p.replace(b'\\', b'')):
        hint = ' — the path contains a literal backslash; use either quotes or \\ escapes, not both'
    die(f'{what} {show(p)} is not a directory{hint}')


def resolve(args, need_source=True):
    target = norm_root(args.target)
    if not os.path.isdir(target):
        not_a_dir('target', target)
    if not need_source:
        return target, None, None, []
    source = norm_root(args.source)
    if not os.path.isdir(source):
        not_a_dir('source', source)
    if source == target or source.startswith(target + b'/'):
        die('source is inside the target')
    prune = [target] if target.startswith(source + b'/') or source == b'/' else []
    label = os.fsencode(args.label) if args.label else (os.path.basename(source) or b'root')
    label = re.sub(rb'[^A-Za-z0-9._-]+', b'_', label).strip(b'._') or b'source'
    return target, source, label, prune


def source_excludes(args, previous=None):
    if previous is not None and not args.exclude and not args.no_default_excludes:
        return previous
    ex = [] if args.no_default_excludes else list(DEFAULT_SOURCE_EXCLUDES)
    return ex + (args.exclude or [])


def cmd_index(args):
    target, *_ = resolve(args, need_source=False)
    st = Store(target)
    if args.exclude is not None:
        st.db.execute('BEGIN')
        st.set_meta('excludes', json.dumps(args.exclude))
        st.db.execute('COMMIT')
    excludes = json.loads(st.meta('excludes', '[]'))
    refresh_index(st, rehash_all=args.rehash_all)
    if args.hash_all:
        hash_target_all(st, args.jobs)
    n, hashed, b = st.db.execute('SELECT count(*), count(hash), coalesce(sum(size),0) FROM files').fetchone()
    dbsize = sum(os.path.getsize(st.dir + b'/index.sqlite' + s) for s in (b'', b'-wal')
                 if os.path.exists(st.dir + b'/index.sqlite' + s))
    print(f'{n:,} files, {hsize(b)}; {hashed:,} fully hashed (others are hashed on demand)')
    print(f'index size {hsize(dbsize)}; excludes: {excludes or "none"}; free space {hsize(st.free_bytes())}')


def cmd_compare(args):
    target, source, label, prune = resolve(args)
    st = Store(target)
    expect = previous_scan_count(st, label, source)
    old_plan = st.plan_path(label, 'plan')
    if os.path.exists(old_plan):
        st.attach(old_plan, 'o')
        try:
            prev_src = st.meta('source', schema='o')
            prev_ex = json.loads(st.meta('excludes', '[]', schema='o'))
        except sqlite3.DatabaseError:
            prev_src, prev_ex = None, None
        st.db.execute('DETACH o')
        if prev_src and os.fsencode(prev_src) != source:
            print(f'WARNING: label {show(label)} was last used for {prev_src}; '
                  f'pass --label to keep sources apart', file=sys.stderr)
    excludes = source_excludes(args)
    whole = ([] if args.no_git_whole else ['.git']) + (args.whole or [])
    stale = ensure_index(st, args)
    st.new_plan(label, 'plan')
    st.attach_cache(label, source)
    st.db.execute('BEGIN')
    for k, v in (('source', os.fsdecode(source)), ('label', os.fsdecode(label)),
                 ('excludes', json.dumps(excludes)), ('whole', json.dumps(whole)),
                 ('created', time.strftime('%Y-%m-%d %H:%M:%S')),
                 ('complete', '0')):
        st.set_meta(k, v, 'p')
    st.db.execute('COMMIT')
    scanned = None if args.rescan else attach_saved_scan(st, label, source, excludes)
    if scanned is None:
        scan_source(st, source, excludes, prune, expect=expect)
        save_scan(st, label, source, excludes)
    else:
        age = time.time() - scanned
        when = time.strftime('%Y-%m-%d %H:%M', time.localtime(scanned))
        if age > SCAN_MAX_AGE:
            print(f'WARNING: reusing the source scan from {when} ({hdur(age)} old). Files added or '
                  f'changed on the source since then are not seen; pass --rescan to scan again '
                  f'(check always rescans).', file=sys.stderr)
        else:
            print(f'reusing the source scan from {when} ({hdur(age)} old); --rescan to scan again')
        scan_source(st, source, excludes, prune, saved_events(st.db), 'loading saved scan')
        st.db.execute('DETACH s')
    terr = classify(st, source, args.jobs, 'compare', label, whole, args.source_jobs)
    st.db.execute('BEGIN')
    st.set_meta('complete', '1', 'p')
    st.db.execute('COMMIT')

    tn, tb = st.db.execute('SELECT count(*), coalesce(sum(size),0) FROM main.files').fetchone()
    header = [f'dedup compare  {time.strftime("%Y-%m-%d %H:%M")}',
              f'target  {show(target)}   (index: {tn:,} files, {hsize(tb)})',
              f'source  {show(source)}   label: {show(label)}',
              f'excluded patterns: {", ".join(excludes) or "none"}',
              f'kept whole: {", ".join(whole) or "nothing"}']
    summ, copy_b, copy_alloc = summary_lines(st, header)
    space = space_lines(st, copy_b, copy_alloc)
    root = build_tree(st.db)
    legend = ['', 'FOLDERS  (✚ all new → copied whole · ✓ nothing to copy · ◐ mixed)']
    details = detail_lines(st, terr)
    base = st.plans + b'/' + label
    with open(base + b'.report.txt', 'w') as f:
        f.write('\n'.join(summ + space + legend + render_tree(root, None, None) + details) + '\n')
    with open(base + b'.plan.tsv', 'w') as f:
        f.write('category\tsize\tsource_path\tdestination\n')
        for cat, forced, size, d, n in st.db.execute(
                f"SELECT s.cat, s.forced, s.size, d.path, s.name FROM p.src s JOIN p.sdirs d ON d.id=s.dir "
                f"WHERE s.cat IN ({','.join('?' * len(COPY_CATS))}) ORDER BY d.path, s.name", COPY_CATS):
            f.write(f'{cat_label(cat, forced)}\t{size}\t{show(rj(d, n))}\t'
                    f'{show(dest_rel(label, cat, rj(d, n)))}\n')
        for d in empty_dirs(st.db):
            f.write(f'empty folder\t0\t{show(d)}/\t{show(dest_rel(label, "new", d))}/\n')

    n_older = sum(1 for _ in st.db.execute("SELECT 1 FROM p.src WHERE cat IN ('older','newer')"))
    print('\n'.join(summ + space + legend + render_tree(root, args.depth, 80)))
    if n_older:
        print(f'\n{n_older:,} files look like other versions of target files — listed in the report.')
    if terr or st.db.execute("SELECT 1 FROM p.src WHERE cat='error' UNION SELECT 1 FROM p.skipped "
                             "WHERE kind='error'").fetchone():
        print('Some files could not be read — listed in the report.')
    print(f'\nfull report: {show(base + b".report.txt")}\ncopy plan:   {show(base + b".plan.tsv")}')
    print(f'next:        dedup.py import {show(target)} {show(source)}'
          + (f' --label {show(label)}' if args.label else ''))


def dest_rel(label, cat, rel):
    base = IMPORT_DIR + b'/' + label + b'/' + FOLDERS[cat]
    return base + b'/' + rel if rel else base


def cmd_import(args):
    target, source, label, _ = resolve(args)
    st = Store(target)
    db = st.db
    plan = st.plan_path(label, 'plan')
    if not os.path.exists(plan):
        die(f'no plan for label {show(label)} — run compare first')
    st.attach(plan, 'p')
    if st.meta('complete', '0', 'p') != '1':
        die('plan is incomplete (compare was interrupted) — run compare again')
    if os.fsencode(st.meta('source', '', 'p')) != source:
        die(f'plan was made for {st.meta("source", "", "p")} — run compare for this source')
    st.attach_cache(label, source)
    stale = ensure_index(st, args)
    gen = int(st.meta('gen'))

    rows = db.execute(f"SELECT s.id, d.path, s.name, s.size, s.mtime, s.hash, s.cat, s.forced FROM p.src s "
                      f"JOIN p.sdirs d ON d.id=s.dir WHERE s.cat IN ({','.join('?' * len(COPY_CATS))}) "
                      f"ORDER BY s.dir, s.ino", COPY_CATS).fetchall()

    # refuse if the source changed since compare
    changed = []
    for sid, d, n, size, mtime, h, cat, forced in rows:
        try:
            s = os.lstat(absp(source, rj(d, n)))
            if s.st_size != size or s.st_mtime_ns != mtime:
                changed.append(rj(d, n))
        except OSError:
            changed.append(rj(d, n))
    if changed:
        die(f'{len(changed)} planned files changed or vanished since compare (e.g. {show(changed[0])}) '
            f'— run compare again')

    present = lambda h: h is not None and db.execute(
        'SELECT 1 FROM main.files WHERE hash=? LIMIT 1', (h,)).fetchone() is not None
    # forced files (empty / whole folder) are copied even though their content exists elsewhere
    todo = [r for r in rows if r[7] is not None or not present(r[5])]
    skipped_present = len(rows) - len(todo)
    need_alloc = sum(alloc(r[3]) for r in todo)
    free = st.free_bytes()
    print(f'to copy: {len(todo):,} files, {hsize(sum(r[3] for r in todo))} '
          f'(≈ {hsize(need_alloc)} on disk); free: {hsize(free)}'
          + (f'; {skipped_present:,} planned files are already on the target now' if skipped_present else ''))
    if free < needed_space(need_alloc) and not args.ignore_space:
        die(f'not enough free space: need ≈ {hsize(needed_space(need_alloc))} incl. margin '
            f'(--ignore-space to try anyway)')

    todo_ids = {r[0] for r in todo}
    db.executemany("UPDATE p.src SET status='already-present' WHERE id=?",
                   [(r[0],) for r in rows if r[0] not in todo_ids])
    dirids = {}
    status = Counter()
    status['already-present'] = skipped_present
    prog = Progress('importing', len(todo), sum(r[3] for r in todo))
    db.execute('BEGIN')
    batch_bytes, batch_t = 0, time.monotonic()
    tmp = None

    def commit_synced():
        sync_target(target)  # data on disk before the index says it is there
        db.execute('COMMIT')
        db.execute('BEGIN')

    try:
        for sid, d, n, size, mtime, h_plan, cat, forced in todo:
            rel = rj(d, n)
            src_abs = absp(source, rel)
            drel_dir = dest_rel(label, cat, d)
            dest_dir = absp(target, drel_dir)
            dest_name = n
            dest = dest_dir + b'/' + dest_name
            tmp = dest + PARTIAL_SUFFIX
            os.makedirs(dest_dir, exist_ok=True)
            try:
                h, nread, s = copy_hashed(src_abs, tmp)
            except OSError as e:
                if e.errno == errno.ENOSPC:
                    raise
                _unlink(tmp)
                tmp = None
                status['read-error'] += 1
                db.execute("UPDATE p.src SET status=?, err=? WHERE id=?", ('read-error', e.strerror, sid))
                prog.add(1, size)
                continue
            st_after = os.lstat(src_abs)
            if (nread != size or st_after.st_size != size or st_after.st_mtime_ns != mtime
                    or (h_plan is not None and h != h_plan)):
                _unlink(tmp)
                tmp = None
                bad = 'hash-mismatch' if nread == size and st_after.st_mtime_ns == mtime else 'changed'
                status[bad] += 1
                db.execute('UPDATE p.src SET status=? WHERE id=?', (bad, sid))
                prog.add(1, size)
                continue
            if os.path.lexists(dest):
                try:
                    same = os.path.isfile(dest) and full_hash(dest) == h
                except OSError:
                    same = False
                if same:
                    _unlink(tmp)
                    tmp = None
                    status['already-present'] += 1
                    db.execute("UPDATE p.src SET status='already-present', dest=? WHERE id=?",
                               (rj(drel_dir, n), sid))
                    prog.add(1, size)
                    continue
                k = 1
                while os.path.lexists(dest_dir + b'/' + n + b'.dedup%d' % k):
                    k += 1
                dest_name = n + b'.dedup%d' % k
                dest = dest_dir + b'/' + dest_name
            os.chmod(tmp, stat.S_IMODE(s.st_mode) | 0o600)
            os.utime(tmp, ns=(s.st_atime_ns, s.st_mtime_ns))
            os.rename(tmp, dest)
            tmp = None
            ph = h if size <= 2 * CHUNK else partial_hash(dest, size)[0]
            ds = os.lstat(dest)
            did = dirids.get(drel_dir)
            if did is None:
                db.execute('INSERT INTO main.dirs(path, gen) VALUES(?,?) ON CONFLICT(path) DO UPDATE SET gen=?',
                           (drel_dir, gen, gen))
                did = dirids[drel_dir] = db.execute('SELECT id FROM main.dirs WHERE path=?',
                                                    (drel_dir,)).fetchone()[0]
            db.execute('INSERT INTO main.files(dir, name, lname, size, mtime, dev, ino, phash, hash, gen) '
                       'VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(dir, name) DO UPDATE SET '
                       'size=excluded.size, mtime=excluded.mtime, dev=excluded.dev, ino=excluded.ino, '
                       'phash=excluded.phash, hash=excluded.hash, gen=excluded.gen',
                       (did, dest_name, dest_name.lower(), size, ds.st_mtime_ns, ds.st_dev, ds.st_ino,
                        ph, h, gen))
            db.execute("UPDATE p.src SET status='copied', hash=?, phash=?, dest=? WHERE id=?",
                       (h, ph, rj(drel_dir, dest_name), sid))
            _cache_put(db, rel, size, mtime, ph, h)
            status['copied'] += 1
            prog.add(1, size)
            batch_bytes += size
            if batch_bytes > 2_000_000_000 or time.monotonic() - batch_t > 30:
                commit_synced()
                batch_bytes, batch_t = 0, time.monotonic()
    except KeyboardInterrupt:
        _unlink(tmp)
        sys.stderr.write('writing remaining data to disk …\n')
        commit_synced()
        db.execute('COMMIT')
        print(f'interrupted after {status["copied"]:,} files ({hsize(prog.b)}) were copied; they are saved.\n'
              f'To continue, run the same command again — copied files are skipped:\n  {resume_cmd()}',
              file=sys.stderr)
        sys.exit(130)
    except OSError as e:
        _unlink(tmp)
        commit_synced()
        db.execute('COMMIT')
        if e.errno == errno.ENOSPC:
            die('target is full — progress is saved; free space and run import again')
        raise
    commit_synced()
    db.execute('COMMIT')
    prog.done()

    for d in empty_dirs(db):
        p = absp(target, dest_rel(label, 'new', d))
        if not os.path.isdir(p):
            os.makedirs(p)
            status['empty folders created'] += 1
    out_dir = absp(target, IMPORT_DIR + b'/' + label)
    os.makedirs(out_dir, exist_ok=True)
    write_manifest(st, label, out_dir + b'/_manifest.tsv')
    report = st.plans + b'/' + label + b'.report.txt'
    if os.path.exists(report):
        with open(report, 'rb') as fi, open(out_dir + b'/_report.txt', 'wb') as fo:
            fo.write(fi.read())
    print('result: ' + (', '.join(f'{v:,} {k}' for k, v in status.items() if v) or 'nothing to do'))
    problems = sum(v for k, v in status.items() if k not in ('copied', 'already-present', 'empty folders created'))
    if problems:
        print(f'{problems} files were NOT copied (see status column in the manifest)')
    print(f'manifest: {show(out_dir + b"/_manifest.tsv")}')
    print(f'next:     dedup.py check {show(target)} {show(source)}' + (f' --label {show(label)}' if args.label else ''))


def copy_hashed(src, tmp):
    """Copy src -> tmp, hashing the bytes read. Returns (hash, bytes, source stat)."""
    _unlink(tmp)
    h = hasher()
    n = 0
    buf = bytearray(BUF)
    mv = memoryview(buf)
    with open_ro(src) as fi, open(tmp, 'xb') as fo:
        s = os.fstat(fi.fileno())
        try:
            os.posix_fadvise(fi.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
        except (AttributeError, OSError):
            pass
        while True:
            k = fi.readinto(buf)
            if not k:
                break
            h.update(mv[:k])
            fo.write(mv[:k])
            n += k
    return h.digest(), n, s


def sync_target(target):
    """syncfs(2) on the target only (os.sync() would flush every mounted disk)."""
    try:
        import ctypes
        libc = ctypes.CDLL(None, use_errno=True)
        fd = os.open(target, os.O_RDONLY)
        try:
            if libc.syncfs(fd) == 0:
                return
        finally:
            os.close(fd)
    except (OSError, AttributeError):
        pass
    os.sync()


def _unlink(p):
    if p:
        try:
            os.unlink(p)
        except FileNotFoundError:
            pass


def write_manifest(st, label, path):
    db = st.db
    with open(path, 'w') as f:
        f.write('category\tstatus\tsize\tmtime\thash\tsource_path\ttarget_path\n')
        for cat, forced, status, size, mtime, h, d, n, dest in db.execute(
                'SELECT s.cat, s.forced, s.status, s.size, s.mtime, s.hash, d.path, s.name, s.dest FROM p.src s '
                'JOIN p.sdirs d ON d.id=s.dir ORDER BY d.path, s.name'):
            tp = dest
            if tp is None and h is not None:
                r = db.execute('SELECT d.path, f.name FROM main.files f JOIN main.dirs d ON d.id=f.dir '
                               'WHERE f.hash=? LIMIT 1', (h,)).fetchone()
                tp = rj(*r) if r else None
            if status is None:
                status = {'present': 'on-target', 'dup': 'on-target' if tp else 'not-copied',
                          'error': 'unreadable'}.get(cat, 'not-copied')
            f.write(f'{cat_label(cat, forced)}\t{status}\t{size}\t{fmt_time(mtime)}\t{h.hex() if h else ""}\t'
                    f'{show(rj(d, n))}\t{show(tp) if tp else ""}\n')


def cmd_check(args):
    target, source, label, prune = resolve(args)
    st = Store(target)
    prev_ex = None
    plan = st.plan_path(label, 'plan')
    if os.path.exists(plan):
        st.attach(plan, 'o')
        try:
            prev_ex = json.loads(st.meta('excludes', 'null', 'o'))
        except sqlite3.DatabaseError:
            pass
        st.db.execute('DETACH o')
    excludes = source_excludes(args, prev_ex)
    expect = previous_scan_count(st, label, source)
    stale = ensure_index(st, args)
    st.new_plan(label, 'check')
    st.attach_cache(label, source)
    scan_source(st, source, excludes, prune, expect=expect)
    terr = classify(st, source, args.jobs, 'check', source_jobs=args.source_jobs)

    db = st.db
    missing = db.execute("SELECT d.path, s.name, s.size, s.cat FROM p.src s JOIN p.sdirs d ON d.id=s.dir "
                         "WHERE s.cat <> 'present' ORDER BY d.path, s.name").fetchall()
    unread_dirs = db.execute("SELECT path, reason FROM p.skipped WHERE kind='error'").fetchall()
    others = Counter(k for (k,) in db.execute("SELECT kind FROM p.skipped WHERE kind<>'error'"))
    n, b = db.execute('SELECT count(*), coalesce(sum(size),0) FROM p.src').fetchone()
    print(f'source {show(source)}: {n:,} files, {hsize(b)} checked against the target '
          f'(excluded patterns: {", ".join(excludes) or "none"})')
    out = st.plans + b'/' + label + b'.check.txt'
    with open(out, 'w') as f:
        for d, nm, size, cat in missing:
            f.write(f'{"unreadable" if cat == "error" else "missing"}\t{size}\t{show(rj(d, nm))}\n')
        for p, r in unread_dirs:
            f.write(f'unreadable\t\t{show(p)}\t{r}\n')
    if others:
        print('not checked: ' + ', '.join(f'{v} {k}' for k, v in sorted(others.items())) +
              ' (symlinks / special files / excluded paths)')
    if not missing and not unread_dirs:
        if stale:
            print(f'✓ SAFE as of the target index from {stale} (--no-reindex). If anything was deleted or\n'
                  f'  changed on the target since then, run check again without --no-reindex before wiping.')
        else:
            print('✓ SAFE: every file on the source has identical content somewhere on the target.')
    else:
        unread = sum(1 for m in missing if m[3] == 'error') + len(unread_dirs)
        print(f'✗ NOT SAFE: {len(missing) - unread + len(unread_dirs):,} files missing on target, '
              f'{unread:,} unreadable files/folders')
        for d, nm, size, cat in missing[:20]:
            print(f'  {"unreadable" if cat == "error" else "missing   "}  {hsize(size):>9}  {show(rj(d, nm))}')
        if len(missing) > 20:
            print(f'  … {len(missing) - 20} more')
        print(f'full list: {show(out)}')
        sys.exit(1)


def resume_cmd():
    import shlex
    argv = list(sys.argv)
    if RUN['reindexed'] and RUN['cmd'] in ('compare', 'import') and '--no-reindex' not in argv:
        argv.append('--no-reindex')  # the refresh finished this run; no need to walk again
    return ' '.join(shlex.quote(a) for a in argv)


def interrupted():
    st = RUN['store']
    if st is not None and st.db.in_transaction:
        st.db.execute('ROLLBACK')  # never keep a half-done refresh; hashes up to the last commit stay
    cmd = RUN['cmd']
    what = {
        'index': 'The index was not changed by the interrupted refresh; hashes already computed are kept.',
        'compare': 'No plan was made yet; hashes already computed are kept and will not be recomputed.',
        'check': 'Nothing was changed.',
    }.get(cmd, 'Progress is saved.')
    hint = ''
    if RUN['reindexed'] and cmd in ('compare', 'import'):
        hint = '\n(the target index refresh had finished, so --no-reindex skips it — fine as long as the\n' \
               ' target is not changed in between)'
    sys.stderr.write(f'interrupted. {what}\nTo continue, run:\n  {resume_cmd()}{hint}\n')
    sys.exit(130)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)

    p = sub.add_parser('index', help='build/refresh the target index')
    p.add_argument('target')
    p.add_argument('--exclude', action='append', metavar='PATTERN',
                   help='glob on name or relative path, e.g. ".snapshots"; replaces the stored list '
                        '(--exclude "" to clear)')
    p.add_argument('--rehash-all', action='store_true', help='forget all stored hashes')
    p.add_argument('--hash-all', action='store_true', help='hash every target file now instead of on demand')
    p.add_argument('--jobs', type=int, default=1, help='parallel hashing threads (SSD: 4-8, btrfs RAID1: 4, single HDD: 1)')
    p.set_defaults(func=cmd_index)

    for name, func, hlp in (('compare', cmd_compare, 'scan a source, report, write copy plan'),
                            ('import', cmd_import, 'copy the planned files to TARGET/_import/<label>/'),
                            ('check', cmd_check, 'verify every source file is on the target')):
        p = sub.add_parser(name, help=hlp)
        p.add_argument('target')
        p.add_argument('source')
        p.add_argument('--label', help='name under _import/ (default: source folder name)')
        p.add_argument('--no-reindex', action='store_true',
                       help='skip the target index refresh (fast, but target changes since the last '
                            'refresh are not seen)')
        if name != 'import':
            p.add_argument('--exclude', action='append', metavar='PATTERN',
                           help='extra glob on name or relative path to skip on the source')
            p.add_argument('--no-default-excludes', action='store_true',
                           help='also scan ' + ', '.join(DEFAULT_SOURCE_EXCLUDES))
            p.add_argument('--jobs', type=int, default=1,
                           help='hashing threads on the target (SSD: 4-8, btrfs RAID1: 4, single HDD: 1)')
            p.add_argument('--source-jobs', type=int, default=1,
                           help='hashing threads on the source (SSD: 4-8, HDD: 1)')
        if name == 'compare':
            p.add_argument('--depth', type=int, default=3, help='folder levels shown on screen (default 3)')
            p.add_argument('--whole', action='append', metavar='PATTERN',
                           help='glob on folder name or relative path: if anything in such a folder is new, '
                                'copy the whole folder (git repos are always kept whole)')
            p.add_argument('--no-git-whole', action='store_true', help="don't keep git repos whole")
            p.add_argument('--rescan', action='store_true',
                           help='walk the source again instead of reusing the saved scan of the last compare')
        if name == 'import':
            p.add_argument('--ignore-space', action='store_true', help='skip the free-space check')
        p.set_defaults(func=func)

    sys.stdout.reconfigure(line_buffering=True)
    args = ap.parse_args()
    if getattr(args, 'exclude', None) == ['']:
        args.exclude = []
    RUN['cmd'] = args.cmd
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, on_signal)
    try:
        args.func(args)
    except KeyboardInterrupt:
        interrupted()


if __name__ == '__main__':
    main()
