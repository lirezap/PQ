## FFM used wrapper of postgresql C library

PQ is an experimental java library that uses FFM to directly access native postgresql C library. This library is
intended to be low level, minimal and with the goal of being high performance. Higher level abstractions can be built
easily by using these low level functions.

---

#### Build Project

First of all you need to clone the project using:

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
    <version>0.0.4</version>
</dependency>
```

Then the library can be used as in:

```java
import ir.jibit.pq.PQX;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

public final class Application {

    public static void main(final String[] args) throws Throwable {
        final var latch = new CountDownLatch(25);

        final var start = System.nanoTime();
        for (int i = 1; i <= 25; i++) {
            Executors.newVirtualThreadPerTaskExecutor().submit(() -> {
                try (final var pqx = new PQX(Path.of("/opt/homebrew/opt/libpq/lib/libpq.dylib"))) {
                    final var conn = pqx.connectDB("postgresql://user:pass@localhost:5432/db").orElseThrow();
                    for (int j = 1; j <= 10000000; j++) {
                        pqx.status(conn);
                    }

                    latch.countDown();
                    pqx.finish(conn);
                } catch (Throwable _) {

                }
            });
        }

        latch.await();
        System.out.println(System.nanoTime() - start);
    }
}
```

To run the project you must provide `--enable-preview` option, like:

```text
java -jar --enable-preview target/Application.jar
```

This sample application creates 25 virtual threads, each thread creates a connection to postgres instance then tried to
call db status function of native `libpq` C library 10 million times. ( 25 * 10,000,000 = 250,000,000 native function
calls). In my m1 macbook air, the whole process takes 832 ms.
