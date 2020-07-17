drop table if exists main.car;
drop table if exists main.users;
drop table if exists main.artist;
drop table if exists main.track;
drop table if exists parent;
drop table if exists parent2;
drop table if exists child1;
drop table if exists child2;
drop table if exists child3;
drop table if exists child4;
drop table if exists album;
drop table if exists song;
drop table if exists test_schema.artist;
drop table if exists test_schema.track;

PRAGMA foreign_keys = ON;
PRAGMA foreign_keys;

CREATE TABLE main.artist(
                       artistid    INTEGER PRIMARY KEY,
                       artistname  TEXT
);
CREATE TABLE main.track(
                      trackid     INTEGER PRIMARY KEY,
                      trackname   TEXT,
                      trackartist INTEGER DEFAULT 0 REFERENCES artist(artistid) ON DELETE SET DEFAULT
);

CREATE INDEX main.trackindex1 ON track(trackartist, trackid);
CREATE INDEX main.trackindex2 ON track(trackartist DESC, trackid);
CREATE UNIQUE INDEX main.unitrack ON track(trackartist, trackid);
CREATE INDEX main.partial_index ON track(trackid) WHERE trackname IS NOT NULL;
CREATE UNIQUE INDEX main.partial_unique_index ON track(trackid) WHERE trackname IS NOT NULL;
