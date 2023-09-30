package ir.jibit.pq.layouts;

import java.lang.foreign.SequenceLayout;
import java.lang.foreign.StructLayout;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.MemoryLayout.PathElement.sequenceElement;
import static java.lang.foreign.MemoryLayout.*;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;

/**
 * Postgresql C library PQconninfoOption definition as memory layout.
 *
 * @author Alireza Pourtaghi
 */
public final class PQConnInfoOption {

    public static final StructLayout PQConnInfoOption = structLayout(
            ADDRESS.withName("keyword"),
            ADDRESS.withName("envvar"),
            ADDRESS.withName("compiled"),
            ADDRESS.withName("val"),
            ADDRESS.withName("label"),
            ADDRESS.withName("dispchar"),
            JAVA_INT.withName("dispsize"),
            paddingLayout(4)
    ).withName("PQconninfoOption");

    public static final SequenceLayout PQConnInfoOptionSequence =
            sequenceLayout(PQConnInfoOption);

    public static final VarHandle PQConnInfoOptionSequence_keyword_varHandle =
            PQConnInfoOptionSequence.varHandle(sequenceElement(), groupElement("keyword"));

    public static final VarHandle PQConnInfoOptionSequence_val_varHandle =
            PQConnInfoOptionSequence.varHandle(sequenceElement(), groupElement("val"));
}
