QewQew [![Maven Central](https://img.shields.io/maven-central/v/tel.schich/qewqew.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22tel.schich%22%20AND%20a:%22qewqew%22)
======

A simple durable queue backed by binary files.

This project is inspired by [Tape](https://github.com/square/tape/), but uses a completely different approach.

Under the hood QewQew implements a linked list of chunk files of a certain maximum capacity with a special file that stores the first chunk's index. Upon opening the queue all chunks are locked exclusively and mapped into memory for efficient random access. The chunk format is defined below. Consumed chunks are removed from disk, an empty queue will remove all files upon closure, new files are pre-allocated to the given chunk size.

File formats
------------

### Types

1. `chunk-ref := 16 bit unsigned integer`  
    a value of `0` is the `NULL_REF` and indicates the absense of a reference
2. `pointer := 32 bit signed integer`
3. `data-length := 16 bit unsigned integer`
4. `data := 0 to 2^16-1 bytes`

### Queue Header

```
|chunk-ref|
```

The queue header file consists only of a reference to the first chunk. If it is the `NULL_REF` it indicates an empty queue.

### Chunk

Chunk files are named like the the queue header file with their chunk index appended and separated by a dot (e.g. `queue.dat.1` if `queue.dat` is the header file).

The file is structured as follows:

```
chunk := head-ptr tail-ptr next-ref payload*
payload := data-length data
head-ptr := pointer
tail-ptr := pointer
next-ref := chunk-ref
```


```
+-----------------+
|                 |
| Queue Header    |
|                 |
|                 |
| |first-chunk|   |
|  +              |
+-----------------+
   |
   |
   |   +-------------------------------------------------------------------+
   |   |                                                                   |
   +-> |  Chunk                                                            |
       |                                                                   |
       |            +---------------------------------------------------+  |
       |            |                                                   |  |
       |            |                                                   v  |
       |  |head-ptr|tail-ptr|next-ref|data-length|data|data-length|data|   |
       |   |                 +        ^                                    |
       |   |                 |        |                                    |
       |   +--------------------------+                                    |
       |                     |                                             |
       |                     |                                             |
       +-------------------------------------------------------------------+
                             |
          +------------------+
          |
          v

       +-------------------------------------------------------------------+
       |                                                                   |
       | Chunk                                                             |
       |                                                                   |
       |                                                                   |
       |                                                                   |
       |                                                                   |
       |                                                                   |
       |                                                                   |
       +-------------------------------------------------------------------+

```
