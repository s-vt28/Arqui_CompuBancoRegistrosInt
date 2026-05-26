# Banco de Registros Interactivo

Simulación de un banco de registros estilo MIPS/RISC-V con acceso concurrente,
**forwarding interno** y **resolución de conflictos de escritura simultánea**.

Implementado en **Java** (compatible con Java 8 en adelante).

---

## ¿Qué hace el proyecto?

Simula un banco de **32 registros** (R0–R31) tipo MIPS/RISC-V al que varios
hilos acceden en paralelo, representando instrucciones que avanzan por el
pipeline (etapas ID → EX → WB).

Se ejecutan **tres escenarios** automáticamente:

### Escenario 1 — Forwarding (bypass RAW)
```
instr#3: ADD R3, R1, R2    (R3 = 5 + 10 = 15)
instr#4: ADD R4, R3, R3    (R4 = R3 + R3 = 30)  ← depende de instr#3
```
`instr#4` necesita el valor de R3 que `instr#3` aún está calculando.
En lugar de esperar al Write Back, lee el resultado directamente del
**buffer de forwarding** (bypass). Se puede observar el STALL y luego
el FORWARD en la traza de salida.

### Escenario 2 — Conflicto de escritura simultánea
```
instr#10: ADD R5, R1, R2   (R5 = 5 + 10 = 15)  ─┐ mismo destino
instr#11: ADD R5, R1, R1   (R5 = 5 + 5  = 10)  ─┘
```
Dos instrucciones escriben el mismo registro. **Gana la más nueva** en
program order (`instr#11`, pipelineId mayor). El intento de `instr#10`
queda registrado como CONFLICT observable. R5 final = 10.

### Escenario 3 — R0 hardwired a 0
Cualquier escritura a R0 se descarta silenciosamente. R0 siempre vale 0
(convención MIPS/RISC-V). Las instrucciones ven el intento marcado como
DESCARTADO en la traza.

---

## Requisitos

- **Java 17** (o superior)

### Instalar Java 17 con Homebrew (macOS)
```bash
brew install openjdk@17

# Agregar al PATH (solo necesario si no está en el PATH por defecto)
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
```

---

## Cómo compilar y ejecutar (terminal — sin NetBeans)

```bash
# Desde la carpeta BancoRegistrosInteractivo/
mkdir -p out
javac -d out src/*.java
java -cp out Main
```

Eso es todo. No se necesita Ant, Maven, ni ninguna dependencia externa.

---

## Cómo ejecutar en NetBeans (opcional)

1. Descarga **Apache NetBeans 17+** desde
   [https://netbeans.apache.org/](https://netbeans.apache.org/)
2. **File → Open Project** → selecciona la carpeta `BancoRegistrosInteractivo`
3. Pulsa **F6** (Run Project)

---

## Estructura del proyecto

```
BancoRegistrosInteractivo/
├── src/
│   ├── Main.java            ← Punto de entrada. Define los 3 escenarios.
│   ├── RegisterFile.java    ← Núcleo: banco de 32 registros con forwarding.
│   ├── Instruction.java     ← Modelo de una instrucción ALU (ADD, SUB, etc.)
│   └── PipelineWorker.java  ← Hilo que simula las etapas ID → EX → WB.
├── out/                     ← Clases compiladas (se genera con javac)
├── build.xml                ← Archivo de Ant (para NetBeans)
└── README.md
```

---

## ¿Qué significa cada línea de la traza de salida?

```
[ciclo    3] [STALL  ] instr#4 esperando R3  (productor: instr#3)
```
`instr#4` detectó que `instr#3` va a producir R3 pero aún no terminó:
se inserta una **burbuja** (stall) hasta que el valor esté disponible.

```
[ciclo    6] [ANNOUNCE] R3  <- 15 (instr#3, disponible vía forwarding)
```
`instr#3` terminó la etapa EX y **anuncia** su resultado al buffer de
forwarding. Los lectores en stall son notificados.

```
[ciclo    7] [FORWARD] R3  -> 15 (instr#3 en vuelo, sin esperar WB)
```
`instr#4` obtiene el valor de R3 **por forwarding**, sin esperar al WB
de `instr#3`. Se resuelve el hazard RAW.

```
[ciclo    9] [CONFLICT] R5 : instr#10 derrotada por instr#11 (program order)
```
Dos instrucciones intentaron hacer WB al mismo tiempo en R5.
`instr#10` (más vieja) pierde; `instr#11` (más nueva) gana y su
valor queda como el estado arquitectónico final del registro.

```
[ciclo   10] [WB     ] R5 : 0 -> 10 (instr#11 commit OK)
```
**Write Back** exitoso: R5 pasa de 0 a 10.

---

## Ejemplo de salida esperada

```
>>> ESCENARIO 1: Forwarding (bypass) en cadena RAW <<<
...
[ciclo    3] [STALL  ] instr#4 esperando R3  (productor: instr#3)
[ciclo    6] [ANNOUNCE] R3  <- 15 (instr#3, disponible vía forwarding)
[ciclo    7] [FORWARD] R3  -> 15 (instr#3 en vuelo, sin esperar WB)
...
  [OK ] R3 == 15
  [OK ] R4 == 30 (forwarding desde instr#3)

>>> ESCENARIO 2: Conflicto de escritura simultánea <<<
...
  [CONFLICT] R5 : instr#10 derrotada por instr#11 (program order)
  [OK ] R5 == 10 (gana la instrucción más joven en program order)

>>> ESCENARIO 3: R0 es hardwired a 0 <<<
...
  [OK ] R0 == 0 a pesar de intentos de escritura
```

> **Nota:** el orden exacto de las líneas puede variar entre ejecuciones
> porque la concurrencia es real. Pero el **resultado final siempre es el
> mismo** y las verificaciones `[OK]` siempre se cumplen.
