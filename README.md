# AWS Commander (Go TUI)

Terminal app estilo commander con doble panel, construida en Go con AWS SDK v2.

## Stack

- Go 1.26
- `aws-sdk-go-v2`
- `bubbletea` + `lipgloss`

## Ejecutar

```bash
cd /Users/javierbelmonte/code/nc-aws
./bin/nc-aws
```

## Navegación por panel

Cada panel navega por niveles con `enter`:

1. `Profiles`
2. `Regions`
3. `Services`
4. `Resources`

Usa `backspace` o `h` para volver al nivel anterior.

## S3: buckets y objetos

- En `s3 > Resources` ves buckets.
- `enter` sobre bucket abre su contenido.
- `enter` sobre prefijo (`folder/`) entra.
- `enter` sobre `../` sube.
- `space` marca/desmarca **bucket** o **object**.

## Copiar / mover / borrar bucket completo

### Copiar (`c`)
- Si seleccionas bucket(s) y el panel destino está dentro de un bucket (`s3` con bucket abierto), copia todos los objetos ahí.
- Si seleccionas 1 bucket y el panel destino está en lista de buckets, pide nuevo nombre de bucket y copia todo.

### Mover (`m`)
- Igual que copiar, pero además borra origen al finalizar.

### Borrar (`d` sobre bucket)
- Borra **todos** los objetos del bucket y luego el bucket.

## Atajos

- `tab`: cambiar panel activo
- `i`: ir a inicio (nivel Profiles) del panel activo
- `j` / `k` o flechas: mover cursor
- `enter`: entrar/abrir
- `h` o `backspace`: subir nivel
- `space`: marcar bucket/object S3
- `c`: copiar (object/bucket)
- `m`: mover (object/bucket)
- `a`: crear permisos cross-account para copy S3 (statements nuevos)
- `u`: revocar último grant creado por la app para ese bucket/principal
- `v`: ver detalle del item
- `e`: ver último error/resultado completo
- `l`: ver historial completo de logs
- `d`: borrar (S3 object/bucket, Lambda)
- `n`: crear bucket S3 (solo en lista de buckets)
- `g`: refrescar
- `q`: salir

## Persistencia de contexto

La app guarda automáticamente el contexto de ambos paneles (nivel, perfil, región, servicio, bucket/prefix, cursor y panel activo) en:

- `~/.nc-aws-session.json`

Al relanzar, restaura donde lo dejaste.

## Registro de grants

Los grants creados con `a` se registran en:

- `~/.nc-aws-policy-registry.json`

Cada entrada guarda SIDs creados, bucket, principal destino y metadatos de revocación. `u` usa ese registro para remover statements luego.

## Notas

- Paneles con tamaño fijo, ajustados al tamaño de terminal.
- Perfiles tomados de `~/.aws/config` y `~/.aws/credentials`.
- Resolución case-insensitive de perfiles (`qivli-dev` / `Qivli-dev`).
- Implementación Bash anterior: `bin/nc-aws.bash_legacy`.
- Implementación Tauri anterior: `ui_legacy_tauri/`.
