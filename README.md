# distribution-service — VetChain

Servicio de distribución. Gestiona los **envíos** de lotes/seriales entre entidades y las
**transferencias de custodia**, registrando quién envía, a quién, cuándo y **quién confirma la
recepción** (historial de movimientos, 3.2).

## Responsabilidad en la arquitectura

Materializa el tramo físico-del-sistema **fabricante → distribuidor → veterinaria**. Es la pieza que
permite que una unidad recorra la cadena de custodia y quede trazada.

- Crea envíos (por lote completo o por seriales concretos).
- Registra la **confirmación de recepción**: solo el receptor (o el emisor) puede confirmarla.
- Mantiene el **historial de movimientos** por lote y por serial.
- Publica `shipment.delivered` / `custody.transferred` para traceability y fraud.

## Tecnologías

| Componente | Versión |
|---|---|
| Node.js | ≥ 18 |
| Express | ^5.2.1 |
| pg | ^8.16.3 |
| amqplib (RabbitMQ) | ^0.10.9 |
| dotenv | ^18.0.1 |

## Instalación y ejecución

**Método oficial (recomendado): Docker Compose**, desde la raíz del proyecto:

```bash
docker compose up -d --build --wait
```

**Ejecución directa** (requiere PostgreSQL y RabbitMQ):

```bash
npm install
node index.js        # o: npm start
```

El servicio escucha en `PORT` (3003 por defecto).

## Endpoints principales

| Método | Ruta | Rol | Descripción |
|---|---|---|---|
| GET | `/` , `/health` | Público | Identificación y estado. |
| POST | `/shipments` | distribuidor, fabricante | Crea un envío (por `quantity` o por `serial_numbers[]`). |
| GET | `/custody/mine` | distribuidor, fabricante, veterinaria | Custodia/movimientos de la entidad autenticada. |
| GET | `/shipments` | Autenticado | Listado de envíos (con ámbito). |
| PATCH | `/shipments/:id/deliver` | distribuidor, veterinaria, fabricante | Confirma la recepción. |
| GET | `/shipments/:id` | Autenticado | Detalle de un envío. |
| POST | `/custody-transfers` | distribuidor | Registra una transferencia de custodia. |
| GET | `/history/batch/:batch_id` | Autenticado | Historial de movimientos de un lote. |
| GET | `/history/serial/:serial` | Autenticado | Historial de movimientos de un serial. |
| GET | `/custody-transfers/serial/:serial` | Autenticado | Transferencias de un serial. |

## Variables de entorno

| Variable | Defecto | Descripción |
|---|---|---|
| `PORT` | `3003` | Puerto de escucha. |
| `DATABASE_URL` | — | Conexión a PostgreSQL. |
| `DB_NAME` | — | Base de datos a usar dentro de la misma instancia. |
| `DB_POOL_MAX` | `10` | Tamaño máximo del pool. |
| `RABBITMQ_URL` | `amqp://localhost:5672` | Conexión AMQP. |
| `JWT_SECRET` | — | Verificación del token. |
| `AUTH_SERVICE_URL` | `http://localhost:3001` | Validación de sesión/ámbito. |
| `MANUFACTURING_URL` | `http://localhost:3002` | Validación del lote de origen. |

## Base de datos

- `shipments` (`id`, `batch_id`, `from_entity`, `to_entity`, `shipped_at`, `status`, `units`, `delivered_at`).
- `custody_transfers` (`id`, `shipment_id`, `serial_number`, `from_entity`, `to_entity`, `timestamp`)
  + índices `idx_transfers_serial`, `idx_shipments_batch`.
- `movement_events` — historial de movimientos (3.2).

## Reglas de negocio

- **Origen = entidad autenticada** (no se toma del cuerpo).
- Un **fabricante solo puede enviar a un distribuidor**.
- **No se envía** si el lote está en recall o vencido → `409`.
- **Confirmación de recepción**: solo el receptor (o el emisor) puede confirmarla; en otro caso → `404`
  "Envío no encontrado".
- Límites: `MAX_SERIALS = 100000` seriales por envío, `MAX_NOTES = 500` caracteres.
- Cuerpo JSON limitado a **5 MB**.

## Eventos

- **Publica** (`vetchain.events`): `shipment.delivered`, `custody.transferred`.
- **Consume**: ninguno.
