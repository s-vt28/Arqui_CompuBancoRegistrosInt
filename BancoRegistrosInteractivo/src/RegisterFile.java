import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Banco de registros estilo MIPS/RISC-V con soporte para acceso concurrente,
 * forwarding interno y resolución de conflictos de escritura simultánea.
 *
 * Modela 32 registros enteros (R0..R31) accesibles desde múltiples hilos
 * que representan instrucciones en distintas etapas del pipeline.
 *
 * Conceptos que implementa:
 *
 *   R0 hardwired a 0: cualquier escritura a R0 se descarta. Siempre devuelve 0.
 *
 *   Lecturas concurrentes: varios hilos pueden leer al mismo tiempo gracias al
 *   ReentrantReadWriteLock. Las escrituras (WB) son exclusivas.
 *
 *   Forwarding (bypass): si una instrucción ya calculó su resultado pero aún no
 *   lo escribió al banco, una instrucción posterior lee el valor directamente del
 *   buffer de forwarding sin esperar el WB. Esto resuelve el hazard RAW.
 *
 *   Stall: si el valor todavía no fue calculado, la instrucción consumidora
 *   espera (stall) hasta que el productor anuncie el valor.
 *
 *   Resolución de escrituras simultáneas: si dos instrucciones hacen WB al mismo
 *   registro a la vez, gana la de mayor pipelineId (más nueva en program order).
 */
public class RegisterFile {

    /** Numero de registros fisicos del banco (convencion MIPS/RISC-V: 32). */
    public static final int NUM_REGISTERS = 32;

    /** Valores actuales de cada registro en el banco fisico. */
    private final long[] registers = new long[NUM_REGISTERS];

    /**
     * Lock de lectura/escritura que modela los puertos del banco:
     * multiples lectores simultaneos (varios ID en paralelo) y un
     * unico escritor a la vez (WB exclusivo).
     */
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    /**
     * Buffer de forwarding: guarda los resultados de instrucciones que
     * terminaron la etapa EX pero aun no hicieron WB. Cuando una instruccion
     * necesita leer un registro que esta aqui, lo obtiene directamente (bypass).
     *
     * Estructura: numero de registro -> InFlightValue (pipelineId + valor calculado).
     */
    private final Map<Integer, InFlightValue> forwardingBuffer = new ConcurrentHashMap<>();

    /**
     * Contador de escrituras descartadas por conflicto (llego tarde, otra
     * instruccion mas nueva ya gano ese registro).
     */
    private final Map<Integer, AtomicLong> writeConflicts = new ConcurrentHashMap<>();

    /**
     * Ultimo pipelineId que realizo un commit exitoso en cada registro.
     * Permite que escritores rezagados detecten que ya fueron derrotados.
     */
    private final Map<Integer, Long> lastCommitted = new ConcurrentHashMap<>();

    /**
     * Registros que tienen un productor declarado como pendiente de calcular
     * su valor. Una lectura sobre uno de estos registros debe esperar (stall)
     * hasta que el productor anuncie el resultado via announceForward.
     */
    private final Map<Integer, Long> pendingProducers = new ConcurrentHashMap<>();

    /** Monitor para coordinar stalls y notificaciones de forwarding. */
    private final Object hazardMonitor = new Object();

    /** Contador de ciclos simulados, solo para hacer legible la traza en consola. */
    private final AtomicLong globalCycle = new AtomicLong(0);

    /**
     * Valor en vuelo dentro del buffer de forwarding.
     * Contiene el pipelineId del productor y el valor calculado que aun
     * no fue escrito al banco fisico.
     */
    private static final class InFlightValue {
        /** Identifica la instruccion productora (menor = mas antigua en program order). */
        final long pipelineId;
        /** Resultado calculado disponible para forwarding. */
        final long value;

        InFlightValue(long pipelineId, long value) {
            this.pipelineId = pipelineId;
            this.value      = value;
        }

        long pipelineId() { return pipelineId; }
        long value()      { return value; }
    }

    public RegisterFile() {
        for (int i = 0; i < NUM_REGISTERS; i++) {
            registers[i] = 0L;
        }
    }

    /**
     * Declara que una instruccion entrara al pipeline y producira un valor
     * para {@code reg}, pero todavia no termino de calcularlo. Cualquier
     * lectura posterior sobre ese registro quedara en stall hasta que
     * llegue el announceForward correspondiente.
     */
    public void declareProducer(int reg, long pipelineId) {
        validateRegister(reg);
        if (reg == 0) return;
        // Gana el productor mas reciente en program order
        pendingProducers.merge(reg, pipelineId, Math::max);
    }

    /**
     * Lee un registro aplicando forwarding y stall segun corresponda.
     *
     *   Si reg == 0, devuelve siempre 0.
     *   Si hay un productor pendiente mas antiguo que este lector,
     *     espera (stall) hasta que anuncie su valor.
     *   Si el registro tiene un valor en el buffer de forwarding, lo usa.
     *   En otro caso, devuelve el valor del banco fisico.
     *
     * @param reg              indice del registro (0..31)
     * @param readerPipelineId pipelineId de la instruccion lectora
     */
    public long read(int reg, long readerPipelineId) throws InterruptedException {
        validateRegister(reg);
        if (reg == 0) return 0L;

        // Deteccion de hazard: hay una instruccion mas antigua produciendo este reg?
        Long producerId = pendingProducers.get(reg);
        if (producerId != null && producerId < readerPipelineId) {
            synchronized (hazardMonitor) {
                while (true) {
                    InFlightValue inFlight = forwardingBuffer.get(reg);
                    if (inFlight != null && inFlight.pipelineId() == producerId) {
                        break;
                    }
                    Long stillPending = pendingProducers.get(reg);
                    if (stillPending == null || !stillPending.equals(producerId)) {
                        // El productor ya commiteo, el valor esta en el banco
                        break;
                    }
                    long c = globalCycle.incrementAndGet();
                    System.out.printf(
                        "[ciclo %4d] [STALL  ] instr#%d esperando R%-2d  (productor: instr#%d)%n",
                        c, readerPipelineId, reg, producerId
                    );
                    hazardMonitor.wait();
                }
            }
        }
        return read(reg);
    }

    /**
     * Lee un registro sin hazard tracking. Util para inicializacion o
     * cuando se sabe que no hay dependencias pendientes.
     */
    public long read(int reg) {
        validateRegister(reg);
        if (reg == 0) return 0L;

        rwLock.readLock().lock();
        try {
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
     * Anuncia que una instruccion termino la etapa EX y su resultado esta
     * disponible para forwarding. Se llama antes de hacer el commit al banco.
     *
     * @param reg        registro destino
     * @param value      valor calculado
     * @param pipelineId identificador de la instruccion productora
     */
    public void announceForward(int reg, long value, long pipelineId) {
        validateRegister(reg);
        if (reg == 0) return;

        // Si ya hay un valor en vuelo para este registro, gana el mas reciente
        forwardingBuffer.merge(reg, new InFlightValue(pipelineId, value),
            (existing, incoming) -> incoming.pipelineId() > existing.pipelineId()
                ? incoming
                : existing);

        synchronized (hazardMonitor) {
            hazardMonitor.notifyAll();
        }

        long c = globalCycle.incrementAndGet();
        System.out.printf(
            "[ciclo %4d] [ANNOUNCE] R%-2d <- %d (instr#%d, disponible via forwarding)%n",
            c, reg, value, pipelineId
        );
    }

    /**
     * Commitea la escritura en el banco fisico (etapa WB).
     *
     * Politica de conflictos: si existe una instruccion mas nueva en program order
     * que tambien escribe este registro, la escritura actual se descarta.
     *
     * @param reg        registro destino
     * @param value      valor a escribir
     * @param pipelineId identificador de la instruccion
     * @return true si la escritura fue aceptada, false si fue derrotada por conflicto
     */
    public boolean commit(int reg, long value, long pipelineId) {
        validateRegister(reg);
        if (reg == 0) {
            long c = globalCycle.incrementAndGet();
            System.out.printf(
                "[ciclo %4d] [WB     ] R0  <- %d (DESCARTADO: R0 es hardwired a 0)%n",
                c, value
            );
            return true;
        }

        rwLock.writeLock().lock();
        try {
            // Busca si hay algun competidor mas joven que ya gano o ganara este registro
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
                // Existe una instruccion mas joven que debe prevalecer en program order
                writeConflicts.computeIfAbsent(reg, k -> new AtomicLong(0))
                              .incrementAndGet();
                long c = globalCycle.incrementAndGet();
                System.out.printf(
                    "[ciclo %4d] [CONFLICT] R%-2d: instr#%d derrotada por instr#%d (program order)%n",
                    c, reg, pipelineId, competidorMasJoven
                );
                return false;
            }

            // Escritura aceptada: actualiza el banco fisico
            long previo = registers[reg];
            registers[reg] = value;
            lastCommitted.put(reg, pipelineId);

            // Limpia el forwarding buffer solo si el valor en vuelo es de esta instruccion
            forwardingBuffer.compute(reg, (k, v) ->
                (v != null && v.pipelineId() == pipelineId) ? null : v);

            // Si esta instruccion era la productora pendiente, ya no lo es
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
     * Devuelve una copia del estado actual del banco de registros.
     * Util para mostrar el estado final y para verificaciones.
     */
    public long[] snapshot() {
        rwLock.readLock().lock();
        try {
            return registers.clone();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** Devuelve un mapa con los conflictos de escritura observados por registro. */
    public Map<Integer, Long> getWriteConflicts() {
        Map<Integer, Long> out = new HashMap<>();
        writeConflicts.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    /** Imprime el estado completo del banco en consola. */
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
            System.out.println("\nConflictos de escritura simultanea observados:");
            conflictos.forEach((reg, count) ->
                System.out.printf("  R%-2d: %d escritura(s) derrotada(s)%n", reg, count));
        } else {
            System.out.println("\nNo se observaron conflictos de escritura.");
        }
    }

    /** Valida que el indice del registro este en rango [0, NUM_REGISTERS). */
    private void validateRegister(int reg) {
        if (reg < 0 || reg >= NUM_REGISTERS) {
            throw new IllegalArgumentException(
                "Registro fuera de rango: R" + reg +
                " (valido: R0..R" + (NUM_REGISTERS - 1) + ")");
        }
    }
}
