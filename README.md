# dedup

Years of backups of backups, spread over a pile of old drives? `dedup` merges them into one place
and copies **only what isn't there yet**.

You point it at your main storage (the *target*) and at an old backup (the *source*). It tells you
which folders are new, which are already on the target, and which are a mix, and how much space the
copy will need. Then it copies just the missing files and confirms when the source is safe to wipe.

- **Never deletes anything.** It only reads the source and only writes to the target's `_import/` and `.dedup/` folders.
  Wiping old drives is your decision.
- **Compares content, not names or dates.** A photo renamed and re-copied five times is still recognised as one photo.
- **Works on folders, not disks.** The target and each source are plain directories: a whole drive, one
  backup folder on it, or a subfolder of your storage. Process them one at a time, in any order.
- **Single Python file, no dependencies** (Python ≥ 3.11, Linux).

## Typical commands

```bash
./dedup.py compare TARGET SOURCE   # see what's new, and how much space it needs; copies nothing
./dedup.py import  TARGET SOURCE   # copy the new files to TARGET/_import/<source-name>/
./dedup.py check   TARGET SOURCE   # "SAFE" = everything in SOURCE now exists in TARGET
```

Example:

```bash
./dedup.py compare /mnt/store "/run/media/me/old drive"
./dedup.py import  /mnt/store "/run/media/me/old drive"
./dedup.py check   /mnt/store "/run/media/me/old drive"    # ✓ SAFE → you may wipe the drive
```

Optional:

```bash
./dedup.py index /mnt/store --exclude .snapshots   # ignore a folder in the target (setting is stored)
```

## Details

### What gets copied, and where

Files go to `TARGET/_import/<source-name>/`, keeping their original folder structure, split into:

| Folder | Contents |
|---|---|
| `new/` | content the target doesn't have |
| `older_versions/` | the target has a *newer* file with the same name in a same-named folder, probably an old draft |
| `newer_on_source/` | the target has an *older* file with that name. Review these by hand. |

- Content that appears several times in the source is copied once.
- Folders where some files already exist on the target arrive incomplete. Only the missing files are copied.
- **Git repos are copied whole** if anything in them is new, so they keep working. Use
  `compare --whole PATTERN` for other folders that must stay complete, e.g. `--whole '*.app'`.
- Empty files (`__init__.py`, `.gitkeep`, …) and empty folders are always copied.
- File timestamps and permissions are preserved. Folder timestamps, xattrs and ACLs are not.
- Not copied: symlinks, device files, unreadable files (reported), and system junk like `$RECYCLE.BIN`,
  `.Trash-*`, `System Volume Information`, `lost+found`. Use `--no-default-excludes` to include those.

`_import/<source-name>/_manifest.tsv` lists every source file and where its content now lives on
the target, so you can always trace where a file went.

### How it works

- Files are compared by a content hash (BLAKE2b). Only files whose size matches another file
  are read at all, first just their beginning and end, then fully if needed. Files with a unique size
  are never hashed.
- The target index lives in `TARGET/.dedup/index.sqlite`, roughly 500 bytes per file (≈ 3 GB for
  6 million files). Every command refreshes it first by checking file sizes and timestamps, without
  reading file contents. So it's fine to keep editing and reorganising the target between runs, and
  moved files keep their hashes.
- `import` hashes each file while copying it and refuses to start if the source changed since
  `compare` or the target lacks space. If interrupted, run it again: it continues where it stopped.
- `check` only says SAFE if every file's content exists on the target **right now**. If you delete
  something on the target later, `check` will notice.

### Tips

- Mount sources read-only if you can: `sudo mount -o remount,ro /run/media/me/old\ drive`.
- Keep btrfs snapshots outside the target folder, or exclude them (`index --exclude .snapshots`).
  Otherwise a file that only survives in a snapshot would count as present.
- `--jobs 4` hashes in parallel. That helps on SSDs; leave it at 1 for spinning disks.
- `--label NAME` picks the folder name under `_import/` (default: the source folder's name).

### Duplicates inside the target: rmlint and duperemove

`dedup` only compares sources against the target. For duplicates *inside* the target, use these:

```bash
# Report only: summary on screen, details in rmlint.json, nothing is changed
rmlint /mnt/store -o summary -o json:rmlint.json

# Also find whole duplicate folders
rmlint /mnt/store -D -o summary -o json:rmlint.json

# Second opinion on a source: what in SOURCE is not in TARGET? (paths after // are the "originals")
rmlint SOURCE // /mnt/store --keep-all-tagged --must-match-tagged -o summary -o uniques:unique.txt
```

To free space without deleting anything (btrfs/XFS only), make identical files share their disk
blocks. Every file, path and timestamp stays as it is:

```bash
# with rmlint: generate a script that clones duplicates, review it, then run it
rmlint /mnt/store -o sh:rmlint.sh -c sh:handler=clone
less rmlint.sh
./rmlint.sh

# or with duperemove (sudo apt install duperemove); -d dedupes, without it it only reports
duperemove -dr --hashfile=/tmp/dr.hash /mnt/store
```

⚠️ By default rmlint writes an `rmlint.sh` that **deletes** duplicates. Running `rmlint` itself is
safe; read the script before you run it. Blocks still held by a btrfs snapshot are only freed once
that snapshot is deleted.
