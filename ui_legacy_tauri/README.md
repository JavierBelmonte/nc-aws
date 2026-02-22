# AWS Commander (MVP)

Aplicación desktop tipo Norton Commander para navegar recursos AWS en dos paneles.

## Implementado

- Stack: `Tauri v2 + React + TypeScript`.
- Lectura de perfiles AWS locales (`~/.aws/config`, `~/.aws/credentials`).
- Contexto independiente por panel: perfil + región + servicio.
- Servicios conectados en backend Rust:
  - `S3`
  - `Lambda`
  - `VPC` (vía EC2)
  - `API Gateway` (v2)
- Shortcuts core:
  - `Tab`, `Enter`, `Ins`
  - `F3`, `F4`, `F5`, `F6`, `F7`, `F8`, `F10`
- Operaciones actuales:
  - `list/get`: S3, Lambda, VPC, API Gateway
  - `create`: S3 (create bucket)
  - `delete`: S3 bucket, Lambda function
  - `copy`: S3 plantilla -> create bucket destino

## Limitaciones actuales

- `update` y parte de `create/delete` están pendientes para VPC/API Gateway/Lambda avanzada.
- Copia de Lambda requiere artefacto de despliegue y no está automatizada aún.
- Política de confirmación/destructive ops es simple (sin diff/dry-run).

## Ejecutar

Requisitos:

- Node.js 20+
- Rust toolchain (`cargo`, `rustc`)
- AWS CLI configurado localmente (perfiles/SSO si aplica)

Comandos:

```bash
npm install
npm run tauri:dev
```

Build:

```bash
npm run build
npm run tauri:build
```

## Comandos backend expuestos

- `list_profiles`
- `list_regions`
- `list_services`
- `list_resources`
- `get_resource`
- `create_resource`
- `update_resource`
- `delete_resource`
- `prepare_copy`
- `execute_copy`
