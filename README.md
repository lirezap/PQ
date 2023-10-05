## FFM used wrapper of postgresql C library

PQ is an experimental java library that uses FFM to directly access native postgresql C library. This library is
intended to be low level, minimal and with the goal of being high performance. Higher level abstractions can be built
easily by using these low level functions.

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
    <version>0.0.12</version>
</dependency>
```

Then the library can be used as in:

```java
package ir.jibit.Application;

import ir.jibit.pq.PQX;
import ir.jibit.pq.enums.ConnStatusType;
import ir.jibit.pq.enums.ExecStatusType;
import ir.jibit.pq.layouts.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.nio.file.Path;

public final class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(final String[] args) throws Throwable {
        try (final var pqx = new PQX(Path.of("/opt/homebrew/opt/libpq/lib/libpq.dylib")); final var arena = Arena.ofConfined()) {
            final var conn = pqx.connectDB("postgresql://user:pass@localhost:5432/db").orElseThrow();
            if (pqx.status(conn) != ConnStatusType.CONNECTION_OK) {
                logger.error("Could not connect to postgresql instance!");
            } else {
                final var ps = PreparedStatement.create(arena);
                PreparedStatement.setStmtName(arena, ps, "insertEvent");
                PreparedStatement.setQuery(arena, ps, "insert into event (type, metadata, entity_table, ts) values ($1, $2, $3, now());");
                PreparedStatement.addTextValue(arena, ps, "TYPE"); // for $1
                PreparedStatement.addTextValue(arena, ps, "Example metadata!"); // for $2
                PreparedStatement.addTextValue(arena, ps, "event"); // for $3
                pqx.prepare(conn, ps);

                final var res = pqx.execPreparedBinaryResult(conn, ps);
                if (!pqx.resultStatus(res).equals(ExecStatusType.PGRES_COMMAND_OK)) {
                    logger.error("Could not execute command!");
                } else {
                    logger.info("Rows affected: {}", pqx.cmdTuplesInt(res));
                }

                pqx.clear(res); // Clear result pointer.
                pqx.finish(conn); // Finish with connection.
            }
        }
    }
}
```

To run the project you must provide `--enable-preview` option, like:

```text
java -jar --enable-preview target/Application.jar
```
