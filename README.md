ddth-queue
==========

DDTH's libary to interact with various queue implementations.

Project home:
[https://github.com/DDTH/ddth-queue](https://github.com/DDTH/ddth-queue)

OSGi environment: ddth-queue modules are packaged as an OSGi bundle.


## License ##

See LICENSE.txt for details. Copyright (c) 2015 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.


## Installation #

Latest release version: `0.1.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-queue</artifactId>
	<version>0.1.0</version>
</dependency>
```

## Usage ##

`ddth-queue` provides a unified APIs to interact with various queue implementations.

Queue flow:



### JDBC Queue Implementation ###

Persistent queue backed by a database system.

