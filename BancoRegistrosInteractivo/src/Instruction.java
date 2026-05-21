/**
 * ============================================================================
 *  Instruction - Representación simplificada de una instrucción del pipeline
 * ============================================================================
 *
 *  Modela una instrucción tipo ALU (ej: ADD rd, rs, rt) con:
 *    - pipelineId : identificador único en program order (menor = más vieja)
 *    - rd         : registro destino (write)
 *    - rs, rt     : registros fuente (read)
 *    - operation  : función que produce el resultado a partir de (vs, vt)
 *
 *  No es una ISA completa: el objetivo es ejercitar el banco de registros,
 *  no implementar un emulador. Por eso operation es un Functional Interface
 *  que recibe dos longs y devuelve uno.
 * ============================================================================
 */
public class Instruction {

    @FunctionalInterface
    public interface AluOp {
        long apply(long a, long b);
    }

    private final long pipelineId;
    private final int rd;
    private final int rs;
    private final int rt;
    private final AluOp operation;
    private final String mnemonic;

    public Instruction(long pipelineId, String mnemonic,
                       int rd, int rs, int rt, AluOp operation) {
        this.pipelineId = pipelineId;
        this.mnemonic   = mnemonic;
        this.rd         = rd;
        this.rs         = rs;
        this.rt         = rt;
        this.operation  = operation;
    }

    public long   pipelineId() { return pipelineId; }
    public int    rd()         { return rd; }
    public int    rs()         { return rs; }
    public int    rt()         { return rt; }
    public AluOp  operation()  { return operation; }
    public String mnemonic()   { return mnemonic; }

    @Override
    public String toString() {
        return String.format("instr#%d %s R%d, R%d, R%d",
            pipelineId, mnemonic, rd, rs, rt);
    }
}
