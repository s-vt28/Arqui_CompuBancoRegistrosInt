import java.util.concurrent.ThreadLocalRandom;

/**
 * Hilo que simula el recorrido de una instruccion por las etapas del pipeline.
 *
 * Etapas que ejecuta:
 *
 *   ID (Instruction Decode): lee los valores de rs y rt del banco de registros.
 *     Si alguno de los registros tiene un productor pendiente mas antiguo,
 *     el hilo se bloquea (stall) hasta que ese valor este disponible.
 *
 *   EX (Execute): aplica la operacion de la ALU sobre los valores leidos y
 *     anuncia el resultado al buffer de forwarding. A partir de ese momento,
 *     instrucciones mas jovenes pueden leerlo sin esperar al WB.
 *
 *   MEM (Memory): no se modela en esta simulacion (no hay loads ni stores).
 *
 *   WB (Write Back): escribe el resultado en el banco fisico. Puede ser
 *     descartado si otra instruccion mas nueva ya gano el mismo registro.
 *
 * Los retardos aleatorios entre etapas son intencionales: estresean al scheduler
 * de hilos y aumentan la probabilidad de exponer condiciones de carrera,
 * lo que permite validar la correctitud del banco bajo concurrencia real.
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
     * Declara al banco que esta instruccion producira un valor para rd.
     * Debe invocarse en program order antes de arrancar el hilo,
     * para que los lectores posteriores detecten el hazard y hagan stall.
     */
    public void issueToPipeline() {
        rf.declareProducer(instr.rd(), instr.pipelineId());
    }

    @Override
    public void run() {
        try {
            // ID: lectura de operandos (con stall si hay hazard pendiente)
            long vs = rf.read(instr.rs(), instr.pipelineId());
            long vt = rf.read(instr.rt(), instr.pipelineId());
            jitter();

            // EX: calculo y anuncio del resultado al buffer de forwarding
            long resultado = instr.operation().apply(vs, vt);
            rf.announceForward(instr.rd(), resultado, instr.pipelineId());
            jitter();

            // WB: commit al banco fisico
            rf.commit(instr.rd(), resultado, instr.pipelineId());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Instruccion interrumpida: " + instr);
        }
    }

    /**
     * Introduce un retardo aleatorio entre etapas para simular
     * tiempos variables de ejecucion y estresar el scheduler de hilos.
     */
    private void jitter() throws InterruptedException {
        if (stageDelayMaxMs > 0) {
            long ms = ThreadLocalRandom.current().nextLong(stageDelayMaxMs + 1);
            if (ms > 0) Thread.sleep(ms);
        }
    }
}
