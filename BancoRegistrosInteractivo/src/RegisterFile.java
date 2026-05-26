import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ============================================================================
 *  RegisterFile - Banco de Registros Interactivo con Forwarding
 * ============================================================================
 *
 *  Simula un banco de registros estilo MIPS/RISC-V de 32 registros enteros
 *  (R0..R31), accesible en paralelo desde múltiples hilos que representan
 *  etapas del pipeline:
 *    - ID (Instruction Decode): etapa que LEE operandos.
 *    - WB (Write Back)        : etapa que ESCRIBE el resultado.
 *
 *  Conceptos de arquitectura de computadores que modela:
 *
 *  1. R0 hardwired a 0 (convención MIPS/RISC-V):
 *     Cualquier escritura a R0 se descarta. Siempre devuelve 0.
 *
 *  2. Lecturas concurrentes (múltiples puertos de lectura):
 *     Varios hilos pueden leer al mismo tiempo gracias al ReentrantReadWriteLock.
 *     Las escrituras (WB) son exclusivas y bloquean a los lectores mientras
 *     se aplican.
 *
 *  3. Forwarding (bypass):
 *     Si una instrucción ya calculó su resultado (etapa EX) pero aún no
 *     lo escribió al banco (etapa WB), una instrucción posterior que
 *     necesite ese valor lo obtiene directamente del "buffer de forwarding"
 *     sin tener que esperar el WB. Esto resuelve el hazard RAW (Read After Write).
 *
 *  4. Stall (burbuja en el pipeline):
 *     Si el valor todavía no fue calculado (la instrucción productora aún
 *     no terminó la etapa EX), la instrucción consumidora espera (stall)
 *     hasta que el valor esté disponible.
 *
 *  5. Resolución de escrituras simultáneas:
 *     Si dos instrucciones intentan hacer WB al mismo registro al mismo tiempo,
 *     gana la instrucción más nueva en program order (mayor pipelineId).
 *     La otra escritura se descarta y queda registrada como "conflicto observable".
 *
 *  Implementación thread-safe con ReentrantReadWriteLock + ConcurrentHashMap.
 * ============================================================================
 */
public class RegisterFile {

    /** Número de registros físicos del banco (convención MIPS/RISC-V: 32). */
    public static final int NUM_REGISTERS = 32;

    /** Arreglo donde se guardan los valores reales de cada registro. */
    private final long[] registers = new long[NUM_REGISTERS];

    /**
     * Lock de lectura/escritura:
     *   - Permite múltiples lectores simultáneos (varios ID en paralelo).
     *   - Solo un escritor a la vez (WB exclusivo).
     * Esto modela un banco de registros con múltiples puertos de lectura
     * y un puerto de escritura.
     */
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    /**
     * Buffer de forwarding:
     * Guarda los resultados de instrucciones que ya terminaron la etapa EX
     * pero aún no hicieron WB. Cuando una instrucción necesita leer un
     * registro que está aquí, se sirve desde el buffer (bypass/forwarding).
     *
     * Estructura: número de registro -> InFlightValue (pipelineId + valor).
     */
    private final Map<Integer, InFlightValue> forwardingBuffer = new ConcurrentHashMap<>();

    /**
     * Mapa de conflictos de escritura simultánea.
     * Cuenta cuántas veces una escritura fue descartada por llegar tarde
     * (otra instrucción más nueva ya había ganado el mismo registro).
     */
    private final Map<Integer, AtomicLong> writeConflicts = new ConcurrentHashMap<>();

    /**
     * Último pipelineId que realizó un commit exitoso en cada registro.
     * Permite detectar si una instrucción rezagada llega a WB DESPUÉS de
     * que otra más nueva ya escribió ese registro, en cuyo caso se descarta.
     */
    private final Map<Integer, Long> lastCommitted = new ConcurrentHashMap<>();

    /** Contador global de "ciclos" (solo para hacer la traza legible). */
    private final AtomicLong globalCycle = new AtomicLong(0);

    /**
     * Representa un valor "en vuelo" dentro del buffer de forwarding.
     * Contiene el identificador de la instrucción que lo produce y el
     * valor calculado que aún no fue escrito al banco físico.
     *
     * NOTA: No se usa la sintaxis "record" de Java 16+ para mantener
     * compatibilidad con Java 8+.
     */
    private static final class InFlightValue {
        /** Identificador de la instrucción productora (menor = más antigua). */
        final long pipelineId;
        /** Valor calculado que está en camino al banco de registros. */
        final long value;

        InFlightValue(long pipelineId, long value) {
            this.pipelineId = pipelineId;
            this.value      = value;
        }

        /** Identificador de la instrucción que produce este valor. */
        long pipelineId() { return pipelineId; }

        /** Valor calculado disponible para forwarding. */
        long value()      { return value; }
    }

    public RegisterFile() {
        // Inicialización: todos los registros a 0 (R0 nunca cambia).
        for (int i = 0; i < NUM_REGISTERS; i++) {
            registers[i] = 0L;
        }
    }

    /**
     * Conjunto de registros que tienen un productor "anunciado" como pendiente
     * de calcular su valor (entró al pipeline pero aún no terminó EX). Una
     * lectura sobre uno de estos registros DEBE esperar (stall) hasta el
     * anuncio del valor, para no leer datos viejos. Modela un pipeline con
     * detección de hazards e inserción de burbujas.
     */
    private final Map<Integer, Long> pendingProducers = new ConcurrentHashMap<>();

    /** Lock auxiliar para esperar/notificar sobre cambios en el forwarding. */
    private final Object hazardMonitor = new Object();

    /**
     * Declara que una instrucción ENTRÓ al pipeline y va a producir un valor
     * para `reg`, pero todavía no ha terminado de calcularlo. Cualquier
     * lectura posterior sobre `reg` se quedará bloqueada (stall) hasta que
     * llegue el announceForward correspondiente.
     *
     * Esta separación entre "voy a escribir" y "ya calculé el valor" es la
     * base de la detección de hazards en pipelines reales.
     */
    public void declareProducer(int reg, long pipelineId) {
        validateRegister(reg);
        if (reg == 0) return;
        // Gana el productor con MAYOR pipelineId (el más reciente en program order)
        // porque será el que defina el valor "vivo" del registro al final.
        pendingProducers.merge(reg, pipelineId, Math::max);
    }

    /**
     * Lee un registro con lógica de forwarding y stalls por hazard.
     *
     * Política:
     *   - Si reg == 0, devuelve siempre 0 (R0 hardwired).
     *   - Si hay un productor declarado para reg con pipelineId menor a
     *     `readerPipelineId` (es decir, una instrucción más antigua que
     *     este lector está produciendo el valor), espera (STALL) hasta que
     *     llegue el announceForward correspondiente y entonces lee por
     *     forwarding.
     *   - Si reg está en el forwardingBuffer (valor en vuelo), devuelve
     *     ese valor (bypass: evita RAW hazard).
     *   - En caso contrario, devuelve el valor almacenado en el banco.
     *
     * @param reg              índice del registro (0..31)
     * @param readerPipelineId pipelineId de la instrucción que está leyendo
     * @return valor leído (con forwarding/stall aplicado si corresponde)
     */
    public long read(int reg, long readerPipelineId) throws InterruptedException {
        validateRegister(reg);
        if (reg == 0) return 0L;

        // ---- Detección de hazard: ¿hay una instrucción más antigua produciendo este reg? ----
        // Si sí, esperar a que su valor aparezca en el forwardingBuffer (stall).
        Long producerId = pendingProducers.get(reg);
        if (producerId != null && producerId < readerPipelineId) {
            synchronized (hazardMonitor) {
                while (true) {
                    InFlightValue inFlight = forwardingBuffer.get(reg);
                    if (inFlight != null && inFlight.pipelineId() == producerId) {
                        // El productor ya anunció su valor: podemos seguir.
                        break;
                    }
                    Long stillPending = pendingProducers.get(reg);
                    if (stillPending == null || !stillPending.equals(producerId)) {
                        // El productor ya commiteó (no está pendiente y no hay anuncio):
                        // el valor está en el banco. Salimos del stall.
                        break;
                    }
                    long c = globalCycle.incrementAndGet();
                    System.out.printf(
                        "[ciclo %4d] [STALL  ] instr#%d esperando R%-2d (productor: instr#%d)%n",
                        c, readerPipelineId, reg, producerId
                    );
                    hazardMonitor.wait();
                }
            }
        }
        return read(reg);
    }

    /**
     * Versión legacy de read sin hazard tracking (útil para inicialización
     * y para mantener compatibilidad con tests simples). Equivalente a
     * leer asumiendo que el lector es la instrucción más antigua del mundo.
     *
     * @param reg índice del registro (0..31)
     * @return valor leído (con forwarding aplicado si corresponde)
     */
    public long read(int reg) {
        validateRegister(reg);

        // R0 siempre devuelve 0 (convención MIPS/RISC-V).
        if (reg == 0) return 0L;

        rwLock.readLock().lock();
        try {
            // Chequeo de forwarding ANTES de leer el banco.
            InFlightValue forwarded = forwardingBuffer.get(reg);
            if (forwarded != null) {
                long c = globalCycle.incrementAndGet();
                System.out.printf(
                    "[ciclo %4d] [FORWARD] R%-2d -> %d (instr#%d en vuelo, sin esperar WB)%n",
                    c, reg, forwarded.value(), forwarded.pipelineId()
                );
                return forwarded.value();
            }

            long val = registers[reg];
            long c = globalCycle.incrementAndGet();
            System.out.printf(
                "[ciclo %4d] [READ   ] R%-2d -> %d%n",
                c, reg, val
            );
            return val;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Anuncia que una instrucción producirá un valor para un registro.
     * Se llama típicamente al final de la etapa EX (o MEM, para loads).
     * El valor queda disponible para forwarding hasta que se invoque commit().
     *
     * @param reg        registro destino
     * @param value      valor calculado
     * @param pipelineId identificador único de la instrucción (program order)
     */
    public void announceForward(int reg, long value, long pipelineId) {
        validateRegister(reg);
        if (reg == 0) return; // escrituras a R0 se descartan

        // Si ya hay otro valor en vuelo para el mismo registro, gana el de
        // menor pipelineId (más antiguo en program order). Esto es coherente
        // con MIPS clásico donde la instrucción más reciente en pipeline
        // sobreescribe el forwarding más viejo.
        forwardingBuffer.merge(reg, new InFlightValue(pipelineId, value),
            (existing, incoming) -> incoming.pipelineId() > existing.pipelineId()
                ? incoming   // la nueva es más reciente -> sobreescribe forwarding
                : existing); // la nueva es más vieja -> ignora

        // Despierta a cualquier lector que estuviera en stall esperando este valor.
        synchronized (hazardMonitor) {
            hazardMonitor.notifyAll();
        }

        long c = globalCycle.incrementAndGet();
        System.out.printf(
            "[ciclo %4d] [ANNOUNCE] R%-2d <- %d (instr#%d, disponible vía forwarding)%n",
            c, reg, value, pipelineId
        );
    }

    /**
     * Compromete (commitea) la escritura en el banco físico (etapa WB).
     * Aplica la política de resolución de conflictos: si llegan dos commits
     * simultáneos al mismo registro, gana el de MENOR pipelineId.
     *
     * @param reg        registro destino
     * @param value      valor a escribir
     * @param pipelineId identificador de la instrucción (program order)
     * @return true si la escritura efectivamente modificó el banco,
     *         false si fue derrotada por otra escritura simultánea (conflicto).
     */
    public boolean commit(int reg, long value, long pipelineId) {
        validateRegister(reg);
        if (reg == 0) {
            // R0 se descarta pero la operación se considera "exitosa"
            long c = globalCycle.incrementAndGet();
            System.out.printf(
                "[ciclo %4d] [WB     ] R0  <- %d (DESCARTADO: R0 es hardwired a 0)%n",
                c, value
            );
            return true;
        }

        rwLock.writeLock().lock();
        try {
            // Detección de conflicto de escritura simultánea sobre el mismo rd.
            //
            // Política de resolución (correcta para pipelines tipo MIPS/RISC-V):
            //   El valor arquitectónicamente visible del registro al terminar
            //   el programa es el de la ÚLTIMA instrucción en program order
            //   que escribió allí (la de MAYOR pipelineId).
            //
            //   Por eso: si llego a commit y existe otro escritor del mismo
            //   reg con pipelineId MAYOR que el mío, mi escritura es derrotada
            //   (el resultado de la más joven prevalece). Si los otros
            //   escritores son TODOS más viejos, yo gano y sobreescribo.
            //
            //   El último commit del registro registra el lastCommittedId para
            //   que escritores rezagados (que llegan después) detecten que
            //   ya fueron derrotados.
            InFlightValue inFlight = forwardingBuffer.get(reg);
            Long pending = pendingProducers.get(reg);

            long competidorMasJoven = -1L;
            if (inFlight != null && inFlight.pipelineId() != pipelineId) {
                competidorMasJoven = inFlight.pipelineId();
            }
            if (pending != null && pending != pipelineId) {
                competidorMasJoven = Math.max(competidorMasJoven, pending);
            }
            Long yaCommiteado = lastCommitted.get(reg);
            if (yaCommiteado != null && yaCommiteado != pipelineId) {
                competidorMasJoven = Math.max(competidorMasJoven, yaCommiteado);
            }

            if (competidorMasJoven > pipelineId) {
                // Existe una instrucción MÁS JOVEN que también escribe este reg.
                // Su escritura debe prevalecer en program order. La nuestra
                // se descarta y queda registrada como conflicto observable.
                writeConflicts.computeIfAbsent(reg, k -> new AtomicLong(0))
                              .incrementAndGet();
                long c = globalCycle.incrementAndGet();
                System.out.printf(
                    "[ciclo %4d] [CONFLICT] R%-2d: instr#%d derrotada por instr#%d (program order)%n",
                    c, reg, pipelineId, competidorMasJoven
                );
                // No tocamos pendingProducers porque puede haber un competidor
                // aún más joven pendiente; lo dejamos para que el ganador limpie.
                return false;
            }

            // Escritura aceptada: actualiza el banco físico.
            long previo = registers[reg];
            registers[reg] = value;
            lastCommitted.put(reg, pipelineId);

            // Limpia el forwarding buffer SI Y SOLO SI el valor en vuelo
            // corresponde a esta misma instrucción. Esto evita borrar
            // valores en vuelo de instrucciones más recientes.
            forwardingBuffer.compute(reg, (k, v) ->
                (v != null && v.pipelineId() == pipelineId) ? null : v);

            // Si esta instrucción era la productora pendiente declarada,
            // ya no lo es (su valor está en el banco). Notificamos por si
            // algún lector estaba en stall.
            pendingProducers.compute(reg, (k, v) ->
                (v != null && v == pipelineId) ? null : v);
            synchronized (hazardMonitor) {
                hazardMonitor.notifyAll();
            }

            long c = globalCycle.incrementAndGet();
            System.out.printf(
                "[ciclo %4d] [WB     ] R%-2d: %d -> %d (instr#%d commit OK)%n",
                c, reg, previo, value, pipelineId
            );
            return true;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Obtiene una instantánea del estado actual del banco de registros.
     * Útil para mostrar el estado final y para tests.
     */
    public long[] snapshot() {
        rwLock.readLock().lock();
        try {
            return registers.clone();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** Devuelve un mapa inmutable con los conflictos de escritura observados. */
    public Map<Integer, Long> getWriteConflicts() {
        Map<Integer, Long> out = new HashMap<>();
        writeConflicts.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    /** Imprime el estado completo del banco de forma legible. */
    public void printState() {
        long[] snap = snapshot();
        System.out.println("\n+-------- Estado final del banco de registros --------+");
        for (int i = 0; i < NUM_REGISTERS; i++) {
            if (i % 4 == 0) System.out.print("| ");
            System.out.printf("R%-2d=%-6d ", i, snap[i]);
            if (i % 4 == 3) System.out.println("|");
        }
        System.out.println("+-----------------------------------------------------+");

        Map<Integer, Long> conflictos = getWriteConflicts();
        if (!conflictos.isEmpty()) {
            System.out.println("\nConflictos de escritura simultánea observados:");
            conflictos.forEach((reg, count) ->
                System.out.printf("  R%-2d: %d escritura(s) derrotada(s)%n", reg, count));
        } else {
            System.out.println("\nNo se observaron conflictos de escritura.");
        }
    }

    /** Valida que el índice del registro esté en rango [0, NUM_REGISTERS). */
    private void validateRegister(int reg) {
        if (reg < 0 || reg >= NUM_REGISTERS) {
            throw new IllegalArgumentException(
                "Registro fuera de rango: R" + reg +
                " (válido: R0..R" + (NUM_REGISTERS - 1) + ")");
        }
    }
}
