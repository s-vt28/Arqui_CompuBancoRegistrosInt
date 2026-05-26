import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * ============================================================================
 *  Main - Punto de entrada: Demostración del Banco de Registros Interactivo
 * ============================================================================
 *
 *  Ejecuta TRES escenarios automáticamente para mostrar cómo funciona el
 *  banco de registros con acceso concurrente:
 *
 *    ESCENARIO 1 - Forwarding (bypass):
 *      Instrucciones encadenadas con dependencia RAW (Read After Write).
 *      El banco entrega el valor "en vuelo" al consumidor sin esperar
 *      a que el productor termine su Write Back.
 *
 *    ESCENARIO 2 - Conflicto de escritura simultánea:
 *      Dos instrucciones distintas escriben el mismo registro destino.
 *      Gana la instrucción más nueva en program order (mayor pipelineId).
 *      La otra queda registrada como "conflicto observable".
 *
 *    ESCENARIO 3 - R0 hardwired:
 *      Cualquier intento de escribir R0 es descartado silenciosamente.
 *      R0 siempre vale 0 (convención MIPS/RISC-V).
 *
 *  Cómo compilar y ejecutar desde la terminal (sin NetBeans ni Ant):
 *    javac -d out src/*.java
 *    java -cp out Main
 * ============================================================================
 */
public class Main {

    /** Separador visual para dividir la salida entre escenarios. */
    private static final String SEPARADOR =
        "\n============================================================\n";

    public static void main(String[] args) throws InterruptedException {
        escenario1_Forwarding();
        System.out.println(SEPARADOR);
        escenario2_ConflictoEscritura();
        System.out.println(SEPARADOR);
        escenario3_R0_Hardwired();
    }

    /**
     * ESCENARIO 1: cadena de dependencias RAW.
     *
     *   instr#1:  ADD R1, R0, #5     (R1 = 5)        -- inicialización
     *   instr#2:  ADD R2, R0, #10    (R2 = 10)
     *   instr#3:  ADD R3, R1, R2     (R3 = R1 + R2)  -- depende de #1 y #2
     *   instr#4:  ADD R4, R3, R3     (R4 = R3 + R3)  -- depende de #3 (forwarding)
     *
     *  Se modelan los "ADD inmediatos" usando R0 (valor 0) + constante en el
     *  registro fuente. Para mantener el modelo simple, lo simulamos como
     *  ADD donde rs=R0 y rt=R(constante prefijada) -> pero más limpio es
     *  usar instrucciones cuyos operandos están preestablecidos.
     *  Aquí inicializamos directamente con commits previos.
     */
    private static void escenario1_Forwarding() throws InterruptedException {
        System.out.println(">>> ESCENARIO 1: Forwarding (bypass) en cadena RAW <<<\n");
        RegisterFile rf = new RegisterFile();

        // Precarga: R1=5, R2=10 (commit directo simulando estado inicial).
        rf.commit(1, 5L, 0);
        rf.commit(2, 10L, 0);

        // instr#3: R3 = R1 + R2  -> debería dar 15
        // instr#4: R4 = R3 + R3  -> depende de instr#3 (forwarding RAW)
        Instruction i3 = new Instruction(3, "ADD", 3, 1, 2, Long::sum);
        Instruction i4 = new Instruction(4, "ADD", 4, 3, 3, Long::sum);

        runConcurrent(rf, Arrays.asList(i3, i4), 30);

        rf.printState();

        // Verificación automática del resultado.
        long[] s = rf.snapshot();
        verificar("R3 == 15", s[3] == 15);
        verificar("R4 == 30 (forwarding desde instr#3)", s[4] == 30);
    }

    /**
     * ESCENARIO 2: dos instrucciones escriben el mismo destino.
     *
     *   instr#10: ADD R5, R1, R2   (R5 = 5 + 10 = 15)
     *   instr#11: ADD R5, R1, R1   (R5 = 5 + 5  = 10)   <- mismo destino
     *
     *  Política (tipo MIPS): el valor arquitectónicamente visible es el de
     *  la instrucción MÁS JOVEN en program order (instr#11). El estado
     *  final de R5 debe ser 10. Si por timing instr#10 commiteara después
     *  que instr#11, su intento queda registrado como conflicto observable.
     */
    private static void escenario2_ConflictoEscritura() throws InterruptedException {
        System.out.println(">>> ESCENARIO 2: Conflicto de escritura simultánea <<<\n");
        RegisterFile rf = new RegisterFile();

        rf.commit(1, 5L, 0);
        rf.commit(2, 10L, 0);

        Instruction i10 = new Instruction(10, "ADD", 5, 1, 2, Long::sum); // 15
        Instruction i11 = new Instruction(11, "ADD", 5, 1, 1, Long::sum); // 10

        runConcurrent(rf, Arrays.asList(i10, i11), 30);

        rf.printState();

        long[] s = rf.snapshot();
        verificar("R5 == 10 (gana la instrucción más joven en program order)", s[5] == 10);
        // Nota: el conflicto solo se registra si instr#10 ejecuta WB DESPUÉS de
        // instr#11. Si por timing instr#10 commitea primero, instr#11 simplemente
        // sobreescribe y no hay conflicto observable. Ambas trazas son válidas.
    }

    /**
     * ESCENARIO 3: R0 hardwired.
     * Intentar escribir R0 debe ser un no-op silencioso.
     */
    private static void escenario3_R0_Hardwired() throws InterruptedException {
        System.out.println(">>> ESCENARIO 3: R0 es hardwired a 0 <<<\n");
        RegisterFile rf = new RegisterFile();

        // Intentos de escritura sobre R0 desde varias instrucciones.
        Instruction i20 = new Instruction(20, "ADD", 0, 1, 1, (a, b) -> 999L);
        Instruction i21 = new Instruction(21, "ADD", 0, 1, 1, (a, b) -> 777L);

        runConcurrent(rf, Arrays.asList(i20, i21), 10);

        rf.printState();
        long[] s = rf.snapshot();
        verificar("R0 == 0 a pesar de intentos de escritura", s[0] == 0);
    }

    /** Lanza N instrucciones en paralelo (una por hilo) y espera a que terminen. */
    private static void runConcurrent(RegisterFile rf,
                                      List<Instruction> instructions,
                                      long stageDelayMaxMs) throws InterruptedException {
        List<PipelineWorker> workers = new ArrayList<>();
        for (Instruction i : instructions) {
            workers.add(new PipelineWorker(i, rf, stageDelayMaxMs));
        }

        // FASE 1: ISSUE en program order. Cada instrucción anuncia al banco
        // que producirá un valor para su rd ANTES de que ningún worker
        // comience a leer operandos. Esto garantiza la detección de hazards.
        for (PipelineWorker w : workers) {
            w.issueToPipeline();
        }

        // FASE 2: ejecución concurrente de las etapas ID/EX/WB.
        ExecutorService pool = Executors.newFixedThreadPool(instructions.size());
        workers.forEach(pool::submit);
        pool.shutdown();
        if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
            pool.shutdownNow();
            throw new IllegalStateException("Timeout esperando hilos del pipeline");
        }
    }

    /** Pequeño helper de aserción legible en consola. */
    private static void verificar(String descripcion, boolean condicion) {
        String marca = condicion ? "OK " : "FALLA";
        System.out.printf("  [%s] %s%n", marca, descripcion);
        if (!condicion) {
            throw new AssertionError("Verificación fallida: " + descripcion);
        }
    }
}
