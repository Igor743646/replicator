## SYS.USER$

| Col name | Description |
|-|-|
| USER# | User unique id |
| NAME | User name |
| SPARE1 | Flags: SuppLogPrimary (1), SuppLogAll (8) |
| ... ||

```json
"106": {
    "user": 106,
    "name": "IGOR",
    "spare1": 0
}
```

## SYS.OBJ$

| Col name | Description |
|-|-|
| OBJ# | Object unique id |
| DATAOBJ# | Data object id? |
| OWNER# | User id of owner |
| NAME | Object name |
| TYPE# | Object type (2 - table, 1 - index?, 13 - xml object type) |
| FLAGS | Flags: Temporary (2), Secondary (16), InMemTemp (32), Dropped (128) |
| ... ||

```json
"81991": {
    "obj": 81991,
    "data_obj": 81991,
    "owner": 106,
    "name": "TEST_BULK",
    "obj_type": 2,
    "flags": 0
}
```

## SYS.TAB$

| Col name | Description |
|-|-|
| OBJ# | Object unique id |
| DATAOBJ# | Data object id? |
| TS# | Tablespace id |
| CLUCOLS | ? |
| FLAGS | Flags: RowMovement (131072), Dependencies (8388608), DelayedSegmentCreation (17179869184) |
| PROPERTY | Flags: Binary (1), Clustered (1024), IOTOverflowSegment (512), IOT2 (536870912), Partitioned (32), Nested (8192) |
| ... ||

```json
"81991": {
    "obj": 81991,
    "data_obj": 81991,
    "tablespace": 4,
    "clu_cols": 0,
    "flags": 1073742353,
    "properties": 536870912
}
```

## SYS.COL$

| Col name | Description |
|-|-|
| CON# | неизвестно |
| OBJ | неизвестно |
| ... ||
| INTCOL# | неизвестно |
| ... ||
| SPARE1 | неизвестно |
| ... ||

## SYS.CCOL$

| Col name | Description |
|-|-|
| CON# | неизвестно |
| OBJ | неизвестно |
| ... ||
| INTCOL# | неизвестно |
| ... ||
| SPARE1 | неизвестно |
| ... ||

