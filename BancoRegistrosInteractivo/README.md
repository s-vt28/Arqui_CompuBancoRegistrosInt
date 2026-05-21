# Banco de Registros Interactivo — Proyecto NetBeans

Simulación de un banco de registros estilo MIPS/RISC-V con acceso concurrente,
**forwarding interno** y **resolución de conflictos de escritura simultánea**.

Empaquetado como **proyecto Java con Ant** (estructura nativa de NetBeans).
No requiere Maven, ni Gradle, ni descargar dependencias.

---

## Requisitos previos

1. **Apache NetBeans 17 o superior** — descarga desde
   [https://netbeans.apache.org/front/main/download/](https://netbeans.apache.org/front/main/download/)
   - En el instalador, asegúrate de marcar el paquete **"Java SE"**.
2. **JDK 17 o superior** — Temurin 17/21 desde
   [https://adoptium.net/](https://adoptium.net/) funciona perfectamente.
   - NetBeans suele detectarlo automáticamente. Si no, en NetBeans:
     **Tools → Java Platforms → Add Platform**.

---

## Cómo abrir el proyecto en NetBeans

1. Descomprime esta carpeta donde quieras (ej: `Documents/BancoRegistrosInteractivo`).
2. Abre NetBeans.
3. Menú **File → Open Project...** (Ctrl+Shift+O).
4. Navega hasta la carpeta `BancoRegistrosInteractivo` (la que contiene
   `build.xml`). El ícono debería aparecer con el "cafecito" de Java naranja.
5. Selecciónala y pulsa **Open Project**.

NetBeans la indexará y aparecerá en el panel "Projects" a la izquierda.

---

## Cómo ejecutar

Hay tres formas, de la más rápida a la más controlada:

### Opción A — botón Run (la más fácil)

- Pulsa **F6**, o
- haz clic derecho sobre el proyecto en el panel "Projects" → **Run**, o
- usa el botón verde ▶ de la barra de herramientas.

La salida aparecerá en la pestaña **Output** abajo, con las trazas
`[READ]`, `[STALL]`, `[FORWARD]`, `[ANNOUNCE]`, `[WB]`, `[CONFLICT]`
y las verificaciones `[OK]` al final de cada escenario.

### Opción B — ejecutar el archivo Main directamente

- Abre `src/Main.java`.
- Pulsa **Shift+F6** (Run File).

### Opción C — depurar paso a paso

- Pon un breakpoint haciendo clic en el margen izquierdo de cualquier línea.
- Pulsa **Ctrl+F5** (Debug Project) o **F5** dentro del depurador.

NetBeans abrirá el panel de "Variables", "Call Stack" y "Threads" — útil
para ver en vivo cómo los hilos del pipeline interactúan con el banco.

---

## Estructura del proyecto

```
BancoRegistrosInteractivo/
├── build.xml                    ← entrada de Ant (no editar)
├── manifest.mf                  ← se incrusta en el JAR
├── nbproject/
│   ├── build-impl.xml           ← targets de Ant (lo gestiona NetBeans)
│   ├── project.xml              ← tipo de proyecto Java SE
│   └── project.properties       ← clase main, paths, versión de Java
├── src/                         ← fuentes
│   ├── Main.java                ← ejecutable (escenarios + verificaciones)
│   ├── RegisterFile.java        ← núcleo concurrente del banco
│   ├── Instruction.java         ← modelo de instrucción ALU
│   └── PipelineWorker.java      ← hilo que simula ID/EX/WB
└── test/                        ← (vacío, listo para JUnit si quieres)
```

---

## ¿Qué hace el código?

Simula un banco de **32 registros** (R0–R31) tipo MIPS/RISC-V, accedido
en paralelo por hilos que representan instrucciones avanzando por el
pipeline (etapas ID → EX → WB).

**Tres escenarios** se ejecutan automáticamente:

1. **Forwarding (bypass)**: `instr#4` necesita el valor que `instr#3`
   está calculando. Hace STALL hasta el anuncio y luego lee por FORWARD
   sin esperar al Write Back. Demuestra cómo se resuelve un **data
   hazard RAW** (Read After Write).

2. **Conflicto de escritura simultánea**: `instr#10` y `instr#11`
   escriben el mismo registro destino. Política tipo MIPS: gana la
   más joven en program order; la otra queda registrada como conflicto.

3. **R0 hardwired**: dos hilos intentan escribir en R0. Las
   escrituras se descartan silenciosamente porque R0 siempre vale 0.

Cada operación se imprime con su ciclo global, así puedes seguir
visualmente el orden de ejecución concurrente. Por ser concurrencia
real, el **orden exacto puede variar entre ejecuciones**, pero el
**resultado final siempre es el mismo** y las verificaciones `[OK]`
lo confirman al final.

---

## Solución de problemas

| Problema | Causa probable | Solución |
|---|---|---|
| "Cannot run project: main class not found" | NetBeans no indexó aún | Espera 30 s o haz **Clean and Build** (Shift+F11) |
| Caracteres raros (¿? en lugar de tildes) | Codificación del Output | Ya manejado con `-Dfile.encoding=UTF-8`. Si persiste: Tools → Options → Misc → Output → UTF-8 |
| "source release 17 requires target release 17" | JDK demasiado viejo | Instala JDK 17+ y configúralo en Tools → Java Platforms |
| El botón Run está gris | No hay main class detectada | Verifica que `project.properties` tenga `main.class=Main` |

---

## Probado

- Compila sin errores con `javac` 17 y 21 (warning cosmético benigno).
- 10 ejecuciones consecutivas vía `ant run`: **10/10 OK**, sin
  flakiness en la concurrencia.
- `ant clean jar` produce un JAR ejecutable standalone que corre con
  `java -jar dist/BancoRegistrosInteractivo.jar`.

---

## Comandos útiles desde terminal (sin NetBeans)

Si en algún momento quieres ejecutar sin abrir el IDE:

```bash
# Compilar
ant compile

# Ejecutar
ant run

# Construir JAR
ant jar
java -jar dist/BancoRegistrosInteractivo.jar

# Limpiar
ant clean
```
