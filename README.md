## Native postgresql access using FFM

PQ is a java library that uses FFM to directly access postgresql. This library is intended to be low level, minimal and
with the goal of being high performance. Higher level abstractions can be built easily by using these low level
functions.

The main usable connectors are PQ, PQX (PQ extended), PQCP (PQ connection pool) and AsyncPQCP (Asynchronous PQCP). PQ is
very low level FFM enabled access connector, it has methods equivalent to postgresql C functions. PQX is based on PQ
with some extended features. PQCP is a connection pool implementation based on PQX and is probably the most important
class of this library. AsyncPQCP is the asynchronous version of PQCP that can be used for asynchronous database access.

---

#### Benchmark

To benchmark the library, I used the following table definition:

```text
-- event table definition.
CREATE TABLE event (
    id           UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    type         VARCHAR(64) NOT NULL,
    metadata     VARCHAR(1024),
    entity_table VARCHAR(64),
    entity_id    UUID,
    ts           TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX event_ts_index ON event (ts DESC);
CREATE INDEX event_type_ts_index ON event (type, ts DESC);
CREATE INDEX event_entity_id_ts_index ON event (entity_id, ts DESC);
CREATE INDEX event_type_entity_id_ts_index ON event (type, entity_id, ts DESC);
```

Then measured important database actions with different setups of PQCP (or it async equivalent).

MacBook Air M1, with postgresql version 15 as docker container with default configuration:

`insertEvents: [count: 1000, parallelism: 10, minPoolSize: 10, maxPoolSize: 25] took 246 ms.`

`insertEvents: [count: 2000, parallelism: 10, minPoolSize: 10, maxPoolSize: 25] took 446 ms.`

`insertEvents: [count: 3000, parallelism: 10, minPoolSize: 10, maxPoolSize: 25] took 687 ms.`

`insertEvents: [count: 4000, parallelism: 10, minPoolSize: 10, maxPoolSize: 25] took 864 ms.`

`insertEvents: [count: 5000, parallelism: 10, minPoolSize: 10, maxPoolSize: 25] took 1086 ms.`

`insertEvents: [count: 1000, parallelism: 25, minPoolSize: 25, maxPoolSize: 25] took 147 ms.`

`insertEvents: [count: 2000, parallelism: 25, minPoolSize: 25, maxPoolSize: 25] took 316 ms.`

`insertEvents: [count: 3000, parallelism: 25, minPoolSize: 25, maxPoolSize: 25] took 396 ms.`

`insertEvents: [count: 4000, parallelism: 25, minPoolSize: 25, maxPoolSize: 25] took 550 ms.`

`insertEvents: [count: 5000, parallelism: 25, minPoolSize: 25, maxPoolSize: 25] took 651 ms.`

`insertEvents: [count: 1000, parallelism: 25, minPoolSize: 50, maxPoolSize: 50] took 147 ms.`

`insertEvents: [count: 2000, parallelism: 25, minPoolSize: 50, maxPoolSize: 50] took 252 ms.`

`insertEvents: [count: 3000, parallelism: 25, minPoolSize: 50, maxPoolSize: 50] took 424 ms.`

`insertEvents: [count: 4000, parallelism: 25, minPoolSize: 50, maxPoolSize: 50] took 503 ms.`

`insertEvents: [count: 5000, parallelism: 25, minPoolSize: 50, maxPoolSize: 50] took 612 ms.`

`insertEventsAsync: [count: 1000, executor-size: 10, minPoolSize: 10, maxPoolSize: 25] took 312 ms.`

`insertEventsAsync: [count: 2000, executor-size: 10, minPoolSize: 10, maxPoolSize: 25] took 524 ms.`

`insertEventsAsync: [count: 3000, executor-size: 10, minPoolSize: 10, maxPoolSize: 25] took 741 ms.`

`insertEventsAsync: [count: 4000, executor-size: 10, minPoolSize: 10, maxPoolSize: 25] took 1015 ms.`

`insertEventsAsync: [count: 5000, executor-size: 10, minPoolSize: 10, maxPoolSize: 25] took 1187 ms.`

`insertEventsAsync: [count: 1000, executor-size: 25, minPoolSize: 25, maxPoolSize: 25] took 203 ms.`

`insertEventsAsync: [count: 2000, executor-size: 25, minPoolSize: 25, maxPoolSize: 25] took 338 ms.`

`insertEventsAsync: [count: 3000, executor-size: 25, minPoolSize: 25, maxPoolSize: 25] took 421 ms.`

`insertEventsAsync: [count: 4000, executor-size: 25, minPoolSize: 25, maxPoolSize: 25] took 580 ms.`

`insertEventsAsync: [count: 5000, executor-size: 25, minPoolSize: 25, maxPoolSize: 25] took 737 ms.`

`insertEventsAsync: [count: 1000, executor-size: 50, minPoolSize: 50, maxPoolSize: 50] took 145 ms.`

`insertEventsAsync: [count: 2000, executor-size: 50, minPoolSize: 50, maxPoolSize: 50] took 240 ms.`

`insertEventsAsync: [count: 3000, executor-size: 50, minPoolSize: 50, maxPoolSize: 50] took 360 ms.`

`insertEventsAsync: [count: 4000, executor-size: 50, minPoolSize: 50, maxPoolSize: 50] took 455 ms.`

`insertEventsAsync: [count: 5000, executor-size: 50, minPoolSize: 50, maxPoolSize: 50] took 561 ms.`

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
    <version>0.0.27</version>
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
