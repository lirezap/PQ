## Native postgresql access using FFM

PQ is a java library that uses FFM to directly access postgresql. This library is intended to be low level, minimal and
with the goal of being high performance. Higher level abstractions can be built easily by using these low level
functions.

The main usable connectors are PQ, PQX (PQ extended) and PQCP (PQ connection pool). PQ is very low level FFM enabled
access connector, it has methods equivalent to postgresql C functions. PQX is based on PQ with some extended features.
PQCP is a connection pool implementation based on PQX and is probably the most important class of this library. 

---

#### Build Library

First of all you need to clone the library using:

```markdown
git clone git@github.com:lirezap/PQ.git
```

Then build it through maven wrapper:

```markdown
./mvnw clean package
```

Or install it locally:

```markdown
./mvnw clean install
```

---

#### Use Library

Before using this library you must install `libpq` library for your operating system to be able to call its
functions from this java library. For example in macOS you can install `libpq` by using this command:

```markdown
brew install libpq
```

Then add PQ dependency into your maven project:

```xml

<dependency>
    <groupId>ir.jibit</groupId>
    <artifactId>pq</artifactId>
    <version>0.0.19</version>
</dependency>
```

Then the library can be used as in:

```java
package ir.jibit.Application;

import ir.jibit.pq.cp.PQCP;
import ir.jibit.pq.layouts.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.nio.file.Path;

public final class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(final String[] args) {
        try (final var cp = new PQCP(Path.of("/opt/homebrew/opt/libpq/lib/libpq.dylib"), "postgresql://user:pass@localhost:5432/db");
             final var arena = Arena.ofShared()) {

            final var ps = PreparedStatement.create(arena);
            PreparedStatement.setStmtName(arena, ps, "insertEvent");
            PreparedStatement.setQuery(arena, ps, "insert into event (type, metadata, entity_table, ts) values ($1, $2, $3, now());");
            PreparedStatement.addTextValue(arena, ps, "TYPE"); // for $1
            PreparedStatement.addTextValue(arena, ps, "Example metadata!"); // for $2
            PreparedStatement.addTextValue(arena, ps, "event"); // for $3

            logger.info("Rows affected: {}", cp.prepareThenExecute(ps));
        } catch (Throwable ex) {
            logger.error(ex.getMessage(), ex);
        }
    }
}
```

To run the project you must provide `--enable-preview` option, like:

```text
java -jar --enable-preview target/Application.jar
```
