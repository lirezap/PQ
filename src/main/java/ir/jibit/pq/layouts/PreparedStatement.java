package ir.jibit.pq.layouts;

import java.lang.foreign.StructLayout;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.MemoryLayout.paddingLayout;
import static java.lang.foreign.MemoryLayout.structLayout;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;

/**
 * Prepared statement definition as memory layout.
 *
 * @author Alireza Pourtaghi
 */
public final class PreparedStatement {

    public static final StructLayout PreparedStatement = structLayout(
            ADDRESS.withName("stmtName"),
            JAVA_INT.withName("nParams"),
            paddingLayout(4),
            ADDRESS.withName("paramValues"),
            ADDRESS.withName("paramLengths"),
            ADDRESS.withName("paramFormats")
    );

    public static final VarHandle PreparedStatement_stmtName_varHandle =
            PreparedStatement.varHandle(groupElement("stmtName"));

    public static final VarHandle PreparedStatement_nParams_varHandle =
            PreparedStatement.varHandle(groupElement("nParams"));

    public static final VarHandle PreparedStatement_paramValues_varHandle =
            PreparedStatement.varHandle(groupElement("paramValues"));

    public static final VarHandle PreparedStatement_paramLengths_varHandle =
            PreparedStatement.varHandle(groupElement("paramLengths"));

    public static final VarHandle PreparedStatement_paramFormats_varHandle =
            PreparedStatement.varHandle(groupElement("paramFormats"));
}
