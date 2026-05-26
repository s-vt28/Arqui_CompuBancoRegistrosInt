/**
 * Representa una instruccion tipo ALU (por ejemplo: ADD rd, rs, rt)
 * que recorre las etapas del pipeline simulado.
 *
 * Campos:
 *   pipelineId : identificador unico en program order (menor = mas vieja)
 *   rd         : registro destino (el que se va a escribir)
 *   rs, rt     : registros fuente (los que se van a leer)
 *   operation  : funcion que calcula el resultado a partir de (vs, vt)
 *   mnemonic   : nombre de la operacion para mostrar en trazas (ej: "ADD")
 *
 * No implementa una ISA completa. El objetivo es ejercitar el banco de
 * registros, no construir un emulador completo. Por eso la operacion es
 * un functional interface que recibe dos longs y devuelve uno.
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
