import java.util.concurrent.ThreadLocalRandom;

/**
 * ============================================================================
 *  PipelineWorker - Hilo que ejecuta una instrucción a través del pipeline
 * ============================================================================
 *
 *  Cada instancia simula las etapas ID -> EX -> MEM -> WB para UNA instrucción:
 *
 *    ID  (Instruction Decode): lee rs y rt del banco de registros.
 *                              Aquí entra en juego el forwarding: si rs o rt
 *                              está en el buffer de forwarding, se lee desde
 *                              allí sin esperar al WB del productor.
 *
 *    EX  (Execute): aplica la operación de la ALU sobre (vs, vt) y
 *                   ANUNCIA el resultado al buffer de forwarding. A partir
 *                   de este momento, instrucciones más jóvenes pueden leer
 *                   el valor sin esperar al WB.
 *
 *    MEM (Memory): se omite en este modelo (no hay loads/stores).
 *
 *    WB  (Write Back): commitea el resultado en el banco físico. Puede
 *                      ganar o perder ante una escritura simultánea de
 *                      otra instrucción más antigua sobre el mismo rd.
 *
 *  Los pequeños sleeps aleatorios son intencionales: estresan al scheduler
 *  de hilos y maximizan la probabilidad de exponer race conditions y
 *  conflictos. Son la herramienta clásica para validar concurrencia.
 * ============================================================================
 */
public class PipelineWorker implements Runnable {

    private final Instruction instr;
    private final RegisterFile rf;
    private final long stageDelayMaxMs;

    public PipelineWorker(Instruction instr, RegisterFile rf, long stageDelayMaxMs) {
        this.instr           = instr;
        this.rf              = rf;
        this.stageDelayMaxMs = stageDelayMaxMs;
    }

    /**
     * Declara al banco que esta instrucción producirá un valor para rd.
     * IMPORTANTE: debe invocarse en program order ANTES de arrancar el
     * hilo, para garantizar que cualquier lector posterior detecte el
     * hazard y haga stall correctamente.
     */
    public void issueToPipeline() {
        rf.declareProducer(instr.rd(), instr.pipelineId());
    }

    @Override
    public void run() {
        try {
            // ---------- ID: lectura de operandos (con stall si hay hazard) ----------
            long vs = rf.read(instr.rs(), instr.pipelineId());
            long vt = rf.read(instr.rt(), instr.pipelineId());
            jitter();

            // ---------- EX: cálculo + anuncio de forwarding ----------
            long resultado = instr.operation().apply(vs, vt);
            rf.announceForward(instr.rd(), resultado, instr.pipelineId());
            jitter();

            // ---------- WB: commit al banco físico ----------
            rf.commit(instr.rd(), resultado, instr.pipelineId());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Instrucción interrumpida: " + instr);
        }
    }

    /**
     * Introduce un pequeño retardo aleatorio entre etapas para estresar
     * el scheduler y reproducir condiciones de carrera realistas.
     */
    private void jitter() throws InterruptedException {
        if (stageDelayMaxMs > 0) {
            long ms = ThreadLocalRandom.current().nextLong(stageDelayMaxMs + 1);
            if (ms > 0) Thread.sleep(ms);
        }
    }
}
