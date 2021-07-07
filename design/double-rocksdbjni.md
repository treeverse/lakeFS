# Loading `rocksdbjni` twice

## Introduction

The lakeFS Spark metadata client uses the `rocksdbjni` library to read and
parse RocksDB-format SSTables.  This fails on Spark-as-a-service providers
(such as DataBricks) whose Spark comes with an old version of this library
-- older versions do not support reading a single SSTable file without the
full DB.  Spark appears to have no plug-in structure that helps here.  All
JARs loaded by Spark or any package included in Spark are shared, and only
the first version accessed is loaded.

The normal resolution in the Spark universe for these issues is to provide
assembled JARs.  During assembly one uses a shader to rename ("shade") the
desired version.  When Spark loads the assembled JAR, the included version
has a different name so it is loaded separately, and everything can mostly
work.

There are multiple issues with shading, notably that shading does not work
with JNI-based libraries.  This happens for two reasons:

1. The binaries ("`.so`s") are included in the JAR, and the Java code must
   be able to load them.  Typically they get stored in the root directory,
   and shading has no effect on them -- the wrong binary is loaded.
1. Java types are encoded inside the symbols inside the binary; shading is
   not able to rename those (it cannot understand native binary formats).

As its name implies, `rocksdbjni` is a JNI-based library, so this applies.
[rocksdb say](https://github.com/facebook/rocksdb/issues/7222):

> You would be best not to relocate the package names for RocksDB when you
> create your Uber jar.

## Shading the unshadeable

This is a design for a solution based on _dynamic loading_.  Specifically,
we utilize the unique role of ClassLoaders in the JVM.  The Spark metadata
client will use a special-purpose ClassLoader to load `rocksdbjni`.  Every
ClassLoader can isolate its loaded classes and their associated resources,
allowing _both_ versions in the same JVM.

The client is loaded by the _Spark ClassLoader_ and loads the `rocksdbjni`
JAR from its _Interfacing ClassLoader_ (the name will be explained).

![client lives in Spark ClassLoader and loads a *different* `rocksdbjni`
using the interfacing ClassLoader](diagrams/spark-with-interfacing-classloader.png)

This creates a new difficulty.  The Interfacing ClassLoader can expose any
class that it loads the client code.  But it is not possible for that code
to access that class directly without using reflection: it has no (static)
types!

Instead, we require the client code to define _interfaces_ that match each
`rocksdbjni` class used.  The Interfacing ClassLoader needs to replace all
occurrences of a type with a matching interface.

## Rules (and limitations)

The Interfacing ClassLoader is constructed with a Map that tells it how to
translate _class names_ in the JAR to _interface_ classes the client knows
about.  When it fetches a class object, it edits that class to expose only
the interfaces.

The JAR _does not implement_ these interfaces at the bytecode level.  This
changes when the class is loaded, so the returned class does implement the
interface.

### Constructors still need reflection

The Instance ClassLoader returns a `java.lang.Class` at runtime, so client
code cannot even write a constructor call.  It has to construct objects by
using reflection.

Instead of creating an object directly:

```scala
val options = new org.rocksdb.Options
```

client code fetches a class and calls its constructor dynamically:

```scala
val optionsClass: Class[_] = interfacingClassLoader.loadClass("org.rocksdb.Options")
val options = optionsClass.newInstance
```

`newInstance` takes any constructor parameters, but obviously typechecking
these will only occur at runtime.

Similarly for static functions, of course: the client must fetch them using
reflection.

### Classes are declared to implement their interfaces

If a class is to be translated to an interface, when the ClassLoader loads
it it marks it as implementing that interface.

This declaration is true _if_ the loaded class does indeed implement those
methods needed by the interface.  This will be checked by the JVM but only
at run-time.

### Return types are safely covariant

The ClassLoader adds no code to translate return types: covariance is safe
and automatic.  However it does change the method to return the interface:
the caller will not be aware of the real type.

Because Java has no variance annotation on any of its types, in general it
is not possible to perform such type translation on complex types (without
performing slow two-way copies and other undesired and unsafe edits).  For
instance, the ClassLoader does nothing when returning container types such
as arrays, functions, or generic containers, that involve types which need
translation.  Fortunately the methods of `rocksdbjni` which we need do not
have methods with such "composite" return types, so this less important in
the first phase.

### Parameter types are (at best) unsafely covariant

When a method parameter has a translated type, the client code cannot pass
that type, it only sees the translated interface type.  So the ClassLoader
translates the parameter type to the interface type.  It also adds code at
the top of the method that casts the incoming value to the original method
parameter type.

This translation is unsafe: if the client passes some other implementation
of the interface (as allowed by the interface!), the downcast fails with a
`ClassCastException`.

For example, if the class `org.rocksdb.Options` is mapped to the interface
`shading.rocksdb.Options`, and the `org.rocksdb.SstFileReader` constructor
has this interface:

```scala
package org.rocksdb

class SstFileReader(options: Options) ...
```

the ClassLoader returns an edited version that looks like it compiled:
```scala
package org.rocksdb

class SstFileReader(options: shading.rocksdb.Options) ... {
  (CHECKCAST options, "Lorg/rocksdb/Options;")
  // ... the actual constructor code ...
}
```

We do not handle composite parameter types, just like for return types.
