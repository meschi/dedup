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
  backup folder on it, or a subfolder of your storage. Two folders on the same disk work too, as long
  as the source isn't inside the target. Process them one at a time, in any order.
- **Single Python file, no dependencies** (Python ≥ 3.11, Linux).

> **⚠️ Use at your own risk.** This is a hobby tool, provided as-is, without any warranty.
> If you lose, corrupt or fry your data, that's on you, not on the author. Keep a backup of
> anything you can't afford to lose, and run `check` before you wipe a drive.
> See [LICENSE](LICENSE).

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

**Interrupted? Just run the same command again.** Ctrl-C (or a closed terminal) stops cleanly and
prints the command to continue. Already computed hashes and already copied files are kept and
skipped. Only the quick scan of file names and sizes starts over. Press Ctrl-C twice to quit at once.

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
- File timestamps and permissions are preserved. Not handled: folder timestamps, xattrs and ACLs.
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
- `--no-reindex` (compare/import/check) skips the index refresh, which can take an hour on large
  HDD targets. Only use it if you haven't changed the target since the last run. Files deleted
  since then would still count as present. Before wiping a drive, run `check` without it.
- `--label NAME` picks the folder name under `_import/` (default: the source folder's name).

### Cleaning up the target: removing duplicates with rmlint

`dedup` never deletes, so after importing, the target still holds duplicates: things you've
sorted into your structure, the leftovers in `_import/`, and old duplicates that were on the target
before. To end up with one clean folder structure, remove them with
[rmlint](https://github.com/sahib/rmlint) (`sudo apt install rmlint`).

A good workflow: build your structure in one folder (here `Sorted/`), move things into it from
`_import/`, and let rmlint delete everything elsewhere that already exists in `Sorted/`.

```bash
cd ~            # rmlint writes rmlint.sh / rmlint.json into the current folder: run it outside the target

# 1. Overview: duplicate files and whole duplicate folders. Changes nothing.
rmlint -T df,dd /mnt/store -o summary -o json:rmlint.json

# 2. Delete in _import/ whatever already exists in Sorted/.
#    Paths after // are "originals": never deleted (-k), and only their duplicates count (-m).
rmlint -T df /mnt/store/_import // /mnt/store/Sorted -k -m -o sh:rmlint.sh -o summary
./rmlint.sh -n          # dry run: lists what would be deleted
./rmlint.sh -p -c       # delete; -p re-checks each file byte by byte first, -c removes folders left empty

# 3. Duplicates anywhere else: -S decides which copy is kept, e.g. shortest path (d), then oldest (m)
rmlint -T df /mnt/store -S dm -o sh:rmlint.sh -o summary
less rmlint.sh          # check which copies it keeps before running it
```

⚠️ Running `rmlint` itself never changes anything. It writes `rmlint.json` and an `rmlint.sh`
script, and only *running that script* deletes files. Always pass `-T df` (duplicate files only):
by default the script also deletes **empty files, empty folders and broken symlinks**.
The next normal `dedup` run notices the deletions automatically.
