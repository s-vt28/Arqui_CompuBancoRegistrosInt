import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Punto de entrada del simulador de banco de registros.
 *
 * Ejecuta tres escenarios para demostrar cómo funciona el banco
 * con acceso concurrente desde múltiples etapas del pipeline:
 *
 *   Escenario 1 - Forwarding (bypass RAW):
 *     Instrucciones encadenadas con dependencia Read After Write.
 *     El banco entrega el valor en vuelo al consumidor sin esperar
 *     a que el productor termine su Write Back.
 *
 *   Escenario 2 - Conflicto de escritura simultánea:
 *     Dos instrucciones escriben el mismo registro destino.
 *     Gana la instrucción más nueva en program order (mayor pipelineId).
 *
 *   Escenario 3 - R0 hardwired:
 *     Cualquier intento de escribir R0 es descartado silenciosamente.
 *     R0 siempre vale 0 (convención MIPS/RISC-V).
 *
 * Para compilar y ejecutar desde la terminal:
 *   javac -d out src/*.java
 *   java -cp out Main
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {
        escenario1_Forwarding();
        System.out.println();
        escenario2_ConflictoEscritura();
        System.out.println();
        escenario3_R0_Hardwired();
    }

    /**
     * Escenario 1: cadena de dependencias RAW.
     *
     *   instr#1:  ADD R1, R0, #5     (R1 = 5)
     *   instr#2:  ADD R2, R0, #10    (R2 = 10)
     *   instr#3:  ADD R3, R1, R2     (R3 = R1 + R2 = 15)
     *   instr#4:  ADD R4, R3, R3     (R4 = R3 + R3 = 30)  depende de #3 por forwarding
     */
    private static void escenario1_Forwarding() throws InterruptedException {
        System.out.println(">>> ESCENARIO 1: Forwarding (bypass) en cadena RAW <<<\n");
        RegisterFile rf = new RegisterFile();

        // Estado inicial: R1=5, R2=10 (representan resultados de instrucciones previas ya commiteadas)
        rf.commit(1, 5L, 0);
        rf.commit(2, 10L, 0);

        // instr#3: R3 = R1 + R2  -> 15
        // instr#4: R4 = R3 + R3  -> depende de instr#3 via forwarding
        Instruction i3 = new Instruction(3, "ADD", 3, 1, 2, Long::sum);
        Instruction i4 = new Instruction(4, "ADD", 4, 3, 3, Long::sum);

        runConcurrent(rf, Arrays.asList(i3, i4), 30);

        rf.printState();

        long[] s = rf.snapshot();
        verificar("R3 == 15", s[3] == 15);
        verificar("R4 == 30 (forwarding desde instr#3)", s[4] == 30);
    }

    /**
     * Escenario 2: dos instrucciones escriben el mismo destino.
     *
     *   instr#10: ADD R5, R1, R2   (R5 = 5 + 10 = 15)
     *   instr#11: ADD R5, R1, R1   (R5 = 5 + 5  = 10)   mismo destino
     *
     * El valor visible al final debe ser el de instr#11 (más nueva en program order).
     * Si instr#10 llega a WB después, su intento queda registrado como conflicto.
     */
    private static void escenario2_ConflictoEscritura() throws InterruptedException {
        System.out.println(">>> ESCENARIO 2: Conflicto de escritura simultanea <<<\n");
        RegisterFile rf = new RegisterFile();

        rf.commit(1, 5L, 0);
        rf.commit(2, 10L, 0);

        Instruction i10 = new Instruction(10, "ADD", 5, 1, 2, Long::sum); // R5 = 15
        Instruction i11 = new Instruction(11, "ADD", 5, 1, 1, Long::sum); // R5 = 10

        runConcurrent(rf, Arrays.asList(i10, i11), 30);

        rf.printState();

        long[] s = rf.snapshot();
        verificar("R5 == 10 (gana la instruccion mas joven en program order)", s[5] == 10);
    }

    /**
     * Escenario 3: R0 hardwired a cero.
     * Cualquier escritura sobre R0 debe ignorarse completamente.
     */
    private static void escenario3_R0_Hardwired() throws InterruptedException {
        System.out.println(">>> ESCENARIO 3: R0 es hardwired a 0 <<<\n");
        RegisterFile rf = new RegisterFile();

        Instruction i20 = new Instruction(20, "ADD", 0, 1, 1, (a, b) -> 999L);
        Instruction i21 = new Instruction(21, "ADD", 0, 1, 1, (a, b) -> 777L);

        runConcurrent(rf, Arrays.asList(i20, i21), 10);

        rf.printState();
        long[] s = rf.snapshot();
        verificar("R0 == 0 a pesar de intentos de escritura", s[0] == 0);
    }

    /**
     * Lanza las instrucciones en paralelo (una por hilo) y espera a que todas terminen.
     * Primero hace el issue en program order para garantizar la detección de hazards.
     */
    private static void runConcurrent(RegisterFile rf,
                                      List<Instruction> instructions,
                                      long stageDelayMaxMs) throws InterruptedException {
        List<PipelineWorker> workers = new ArrayList<>();
        for (Instruction i : instructions) {
            workers.add(new PipelineWorker(i, rf, stageDelayMaxMs));
        }

        // Issue en program order: cada instrucción anuncia al banco que producirá
        // un valor para su rd antes de que ningún hilo empiece a leer operandos.
        for (PipelineWorker w : workers) {
            w.issueToPipeline();
        }

        // Ejecucion concurrente de las etapas ID / EX / WB
        ExecutorService pool = Executors.newFixedThreadPool(instructions.size());
        workers.forEach(pool::submit);
        pool.shutdown();
        if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
            pool.shutdownNow();
            throw new IllegalStateException("Timeout esperando hilos del pipeline");
        }
    }

    /** Imprime el resultado de una verificacion en consola. Lanza excepcion si falla. */
    private static void verificar(String descripcion, boolean condicion) {
        String marca = condicion ? "OK " : "FALLA";
        System.out.printf("  [%s] %s%n", marca, descripcion);
        if (!condicion) {
            throw new AssertionError("Verificacion fallida: " + descripcion);
        }
    }
}
