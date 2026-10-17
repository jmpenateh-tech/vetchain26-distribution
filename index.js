import "dotenv/config";
import express from "express";
import crypto from "crypto";
import { pool, initDb, publish, jsonError, fail, bodyDefault, installErrorHandlers,
  authenticate, auditTrail, requireRole, isAdmin, isMaster, sameEntity, callService, withScheme, AUTH_URL, getScope, ScopeError } from "./common.js";

const app = express();
const PORT = process.env.PORT || 3003;
const MANUFACTURING = withScheme(process.env.MANUFACTURING_URL || "localhost:3002");
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const MAX_SERIALS = 100000;
const MAX_NOTES = 500;
const UNKNOWN_MANUFACTURER = "Fabricante no registrado";

app.disable("x-powered-by");
app.use(express.json({ limit: "5mb" })); // un envío puede llevar hasta 100.000 seriales
app.use(bodyDefault);

await initDb(`
CREATE TABLE IF NOT EXISTS shipments(
  id UUID PRIMARY KEY,
  batch_id UUID NOT NULL,
  from_entity TEXT NOT NULL,
  to_entity TEXT NOT NULL,
  shipped_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status TEXT NOT NULL DEFAULT 'en_transito');
CREATE TABLE IF NOT EXISTS custody_transfers(
  id UUID PRIMARY KEY,
  shipment_id UUID NOT NULL,
  serial_number TEXT NOT NULL,
  from_entity TEXT NOT NULL,
  to_entity TEXT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS idx_transfers_serial ON custody_transfers(serial_number);
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS units INT NOT NULL DEFAULT 0;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_shipments_batch ON shipments(batch_id);`);

// 3.2: registro de movimientos (quién envió, a quién, cuándo, cuánto y quién confirmó la
// recepción). Migración NO destructiva: solo añade columnas, una tabla de eventos e
// índices. Los envíos anteriores se conservan; los datos que entonces no se guardaban
// (persona que registró, quién confirmó) quedan en null y la interfaz los marca como
// «no registrado (anterior a 3.2)».
await initDb(`
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'envio';
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS from_role TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS to_role TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS lot_number TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS product_name TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS notes TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS sent_by_id TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS sent_by_name TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS sent_by_role TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS received_by_id TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS received_by_name TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS received_by_role TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS reception_by TEXT;
ALTER TABLE shipments ADD COLUMN IF NOT EXISTS reception_notes TEXT;
ALTER TABLE custody_transfers ADD COLUMN IF NOT EXISTS seq BIGSERIAL;
ALTER TABLE custody_transfers ADD COLUMN IF NOT EXISTS sent_by_id TEXT;
ALTER TABLE custody_transfers ADD COLUMN IF NOT EXISTS sent_by_name TEXT;
ALTER TABLE custody_transfers ADD COLUMN IF NOT EXISTS sent_by_role TEXT;
CREATE INDEX IF NOT EXISTS idx_transfers_shipment ON custody_transfers(shipment_id);
CREATE INDEX IF NOT EXISTS idx_transfers_serial_order ON custody_transfers(serial_number, timestamp DESC, seq DESC);
UPDATE shipments SET from_role='distribuidor' WHERE from_role IS NULL AND kind='envio';
CREATE TABLE IF NOT EXISTS movement_events(
  id BIGSERIAL PRIMARY KEY,
  shipment_id UUID NOT NULL,
  batch_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  actor_id TEXT,
  actor_name TEXT NOT NULL,
  actor_role TEXT,
  from_entity TEXT,
  to_entity TEXT,
  units INT,
  notes TEXT,
  at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS idx_movement_events_shipment ON movement_events(shipment_id);
CREATE INDEX IF NOT EXISTS idx_movement_events_batch ON movement_events(batch_id);`);

app.get("/", (_, res) => res.json({ service: "distribution-service", status: "ok" }));
app.get("/health", async (_, res) => {
  try { await pool.query("select 1"); res.json({ status: "ok" }); }
  catch { res.status(503).json({ status: "down" }); }
});

// 2.7: todas las rutas (salvo / y /health) exigen una sesión válida (C1).
// 2.8: registro de auditoría (va antes de authenticate para anotar también los 401).
// 3.2: los mensajes dicen quién envió, a quién, cuántas unidades y de qué lote.
app.use(auditTrail("distribution", [
  ["POST", /^\/shipments\/?$/, "shipment.created", (b, r) => ({ target: r.to_entity || b.to_entity, message: `Registró un envío de ${r.units ?? r.transfers ?? b.quantity ?? (b.serial_numbers || []).length} unidades${r.lot_number ? ` del lote ${r.lot_number}` : ""} de ${r.from_entity || "?"} hacia ${r.to_entity || b.to_entity || "?"}${r.factory_pickup ? ` (incluye el retiro en fábrica de ${r.factory_pickup.units} unidades de ${r.factory_pickup.from_entity})` : ""}` })],
  ["PATCH", /^\/shipments\/[^/]+\/deliver\/?$/, "shipment.delivered", (b, r) => ({ target: r.to_entity, message: `Confirmó la recepción de ${r.units ?? "?"} unidades${r.lot_number ? ` del lote ${r.lot_number}` : ""} enviadas por ${r.from_entity || "?"} a ${r.to_entity || "destino"}${r.reception_by === "remitente" ? " (como remitente)" : ""}` })],
  ["POST", /^\/custody-transfers\/?$/, "custody.transferred", (b, r) => ({ target: r.serial_number || b.serial_number, message: `Transfirió la unidad ${r.serial_number || b.serial_number || ""}${r.to_entity ? ` a ${r.to_entity}` : ""}` })],
]));
app.use(authenticate);

// -----------------------------------------------------------------------------
// Validaciones de custodia (2.7, C3 y A1; ampliadas en 3.2)
// -----------------------------------------------------------------------------
class Rejection extends Error {
  constructor(status, message, extra) { super(message); this.status = status; this.extra = extra; }
}
const reject = (res, e, fallback) => (e instanceof Rejection ? res.status(e.status).json({ error: e.message, ...(e.extra || {}) }) : fail(res, e, fallback));
const actor = (req) => ({ id: req.auth.id === undefined || req.auth.id === null ? null : String(req.auth.id), name: req.auth.name, role: req.auth.role });
const cleanNotes = (v) => (typeof v === "string" ? v.trim().slice(0, MAX_NOTES) || null : null);

async function fetchBatch(req, batchId) {
  if (!UUID_RE.test(String(batchId || ""))) throw new Rejection(400, "Selecciona un lote válido");
  let r;
  try { r = await callService(MANUFACTURING, `/batches/${batchId}`, req); }
  catch { throw new Rejection(503, "No se pudo validar el lote con Manufacturing. Intenta de nuevo."); }
  if (r.status === 404) throw new Rejection(404, "El lote no existe");
  if (!r.ok) throw new Rejection(503, "No se pudo validar el lote con Manufacturing. Intenta de nuevo.");
  return r.data;
}

// El lote debe existir en Manufacturing, no estar en recall ni vencido.
async function loadBatch(req, batchId) {
  const batch = await fetchBatch(req, batchId);
  if (batch.status === "recall") throw new Rejection(409, "El lote está en recall: no se puede distribuir");
  if (String(batch.expiry_date) < new Date().toISOString().slice(0, 10)) throw new Rejection(409, "El lote está vencido: no se puede distribuir");
  return batch;
}

// Unidades del lote registradas en Manufacturing (serial -> estado).
async function batchSerials(req, batchId) {
  let r;
  try { r = await callService(MANUFACTURING, `/serial-numbers/batch/${batchId}?limit=${MAX_SERIALS}`, req, { timeout: 15000 }); }
  catch { throw new Rejection(503, "No se pudieron validar las unidades con Manufacturing. Intenta de nuevo."); }
  if (!r.ok) throw new Rejection(503, "No se pudieron validar las unidades con Manufacturing. Intenta de nuevo.");
  return new Map((r.data || []).map((s) => [s.serial_code, s.status]));
}

// Cada serial debe existir, pertenecer al lote y no estar retirado. Antes (2.6)
// se aceptaba cualquier texto y Traceability lo daba por auténtico (C3).
function checkSerialsBelong(known, serials) {
  const unknown = serials.filter((s) => !known.has(s));
  if (unknown.length) {
    throw new Rejection(409, `${unknown.length} número(s) de serie no pertenecen a este lote o no están registrados (p. ej. ${unknown[0]}). Solo se pueden enviar unidades fabricadas en VetChain.`);
  }
  const retired = serials.filter((s) => known.get(s) === "retirado");
  if (retired.length) throw new Rejection(409, `${retired.length} unidad(es) están retiradas (recall) y no se pueden enviar`);
}

// Último movimiento de cada unidad (orden total: fecha y secuencia de inserción).
async function lastTransfers(serials, db = pool) {
  if (!serials.length) return new Map();
  const r = await db.query(
    `select distinct on (ct.serial_number) ct.serial_number, ct.to_entity, ct.shipment_id, s.status
       from custody_transfers ct join shipments s on s.id = ct.shipment_id
      where ct.serial_number = any($1::text[])
      order by ct.serial_number, ct.timestamp desc, ct.seq desc`, [serials]);
  return new Map(r.rows.map((x) => [x.serial_number, x]));
}

// Clasifica las unidades según quién puede enviarlas (3.2).
//   factory: nunca salieron del fabricante
//   held:    la última transferencia fue hacia esta cuenta y su recepción está confirmada
//   pending: van hacia esta cuenta pero aún no ha confirmado la recepción
//   other:   las tiene otra entidad
async function classify(entity, serials) {
  const last = await lastTransfers(serials);
  const out = { factory: [], held: [], pending: [], other: [] };
  for (const s of serials) {
    const t = last.get(s);
    if (!t) out.factory.push(s);
    else if (!sameEntity(t.to_entity, entity)) out.other.push({ serial: s, holder: t.to_entity });
    else if (t.status === "entregado") out.held.push(s);
    else out.pending.push(s);
  }
  return out;
}

// Qué unidades puede enviar la cuenta que inicia sesión.
//   fabricante:   solo las de SUS lotes que siguen en fábrica.
//   distribuidor: las que recibió (con la recepción confirmada) y, por compatibilidad con
//                 versiones anteriores, las que siguen en fábrica: en ese caso se registra
//                 también la salida de fábrica (fabricante -> distribuidor), así la cadena
//                 queda completa aunque el fabricante no haya registrado el despacho.
function eligibleFor(req, batch, cls) {
  const role = req.auth.role;
  if (role === "fabricante") return { fromFactory: cls.factory, fromStock: [] };
  return { fromFactory: cls.factory, fromStock: cls.held };
}

function custodyProblem(req, cls) {
  if (cls.other.length) return new Rejection(409, `${cls.other.length} unidad(es) no están bajo tu custodia (la tiene ${cls.other[0].holder}).`);
  if (cls.pending.length) return new Rejection(409, `${cls.pending.length} unidad(es) están en camino hacia ti: confirma primero la recepción del envío y luego podrás reenviarlas.`);
  if (req.auth.role === "fabricante" && (cls.held.length)) return new Rejection(409, "Esas unidades ya salieron de fábrica");
  return null;
}

// El destino debe ser una cuenta activa registrada (distribuidor o veterinaria).
// 3.2: un fabricante solo envía a distribuidores.
async function checkDestination(req, toEntity) {
  let r;
  try { r = await callService(AUTH_URL, "/entities", req); }
  catch { throw new Rejection(503, "No se pudo validar el destino. Intenta de nuevo."); }
  if (!r.ok) throw new Rejection(503, "No se pudo validar el destino. Intenta de nuevo.");
  const match = (r.data || []).find((e) => sameEntity(e.name, toEntity));
  if (!match) throw new Rejection(400, "El destino debe ser una cuenta registrada y activa (distribuidor o veterinaria)");
  if (req.auth.role === "fabricante" && match.role !== "distribuidor") throw new Rejection(400, "El fabricante entrega sus lotes a un distribuidor: elige una cuenta de distribuidor como destino");
  return { name: match.name, role: match.role || null };
}

async function insertShipment(client, s) {
  return (await client.query(
    `insert into shipments(id,batch_id,from_entity,to_entity,units,kind,from_role,to_role,lot_number,product_name,notes,
                           sent_by_id,sent_by_name,sent_by_role,status,shipped_at,delivered_at,received_by_id,received_by_name,received_by_role,reception_by,reception_notes)
     values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15, now() - make_interval(secs => $16::float8), case when $17::boolean then now() - make_interval(secs => $16::float8) end, $18,$19,$20,$21,$22) returning *`,
    [crypto.randomUUID(), s.batch_id, s.from_entity, s.to_entity, s.units, s.kind, s.from_role, s.to_role, s.lot_number, s.product_name, s.notes,
      s.by.id, s.by.name, s.by.role, s.status || "en_transito", s.offsetSecs || 0, !!s.delivered,
      s.received?.id ?? null, s.received?.name ?? null, s.received?.role ?? null, s.received ? s.reception_by : null, s.reception_notes ?? null])).rows[0];
}
async function insertTransfers(client, shipment, serials, by) {
  return (await client.query(
    `insert into custody_transfers(id,shipment_id,serial_number,from_entity,to_entity,timestamp,sent_by_id,sent_by_name,sent_by_role)
     select gen_random_uuid(), $1, s, $2, $3, $5, $6, $7, $8 from unnest($4::text[]) with ordinality as u(s, n) order by n returning *`,
    [shipment.id, shipment.from_entity, shipment.to_entity, serials, shipment.shipped_at, by.id, by.name, by.role])).rows;
}
async function insertEvent(client, shipment, type, by, notes = null) {
  await client.query(
    `insert into movement_events(shipment_id,batch_id,event_type,actor_id,actor_name,actor_role,from_entity,to_entity,units,notes)
     values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [shipment.id, shipment.batch_id, type, by.id, by.name, by.role, shipment.from_entity, shipment.to_entity, shipment.units, notes]);
}

// Registra (en la transacción del cliente) el retiro en fábrica implícito y el envío.
async function registerMovement(client, req, { batch, to, serialsFactory, serialsStock, notes }) {
  const by = actor(req);
  const base = { batch_id: batch.id, lot_number: batch.product_id || null, product_name: batch.product_name || null };
  const published = [];
  let pickup = null;
  const all = [...serialsStock, ...serialsFactory];
  if (req.auth.role === "distribuidor" && serialsFactory.length) {
    // Salida de fábrica que el fabricante no registró: la registra el distribuidor que se
    // lleva las unidades. Queda recibida por él mismo (las tiene en la mano) y 1 ms antes
    // que el envío para que el orden de la cadena sea inequívoco.
    pickup = await insertShipment(client, {
      ...base, from_entity: batch.manufacturer || UNKNOWN_MANUFACTURER, to_entity: by.name, units: serialsFactory.length, kind: "retiro_fabrica",
      from_role: "fabricante", to_role: "distribuidor", notes: "Retiro en fábrica registrado por el distribuidor (el fabricante no registró el despacho)",
      by, status: "entregado", offsetSecs: 0.001, delivered: true, received: by, reception_by: "destinatario",
    });
    const t = await insertTransfers(client, pickup, serialsFactory, by);
    await insertEvent(client, pickup, "envio_registrado", by, pickup.notes);
    await insertEvent(client, pickup, "recepcion_confirmada", by, "Recibidas en fábrica por el distribuidor");
    published.push(...t.map((x) => ({ transfer: x, batch_id: batch.id })));
  }
  const shipment = await insertShipment(client, {
    ...base, from_entity: by.name, to_entity: to.name, units: all.length, kind: "envio",
    from_role: by.role, to_role: to.role, notes, by,
  });
  const transfers = await insertTransfers(client, shipment, all, by);
  await insertEvent(client, shipment, "envio_registrado", by, notes);
  published.push(...transfers.map((x) => ({ transfer: x, batch_id: batch.id })));
  return { shipment, pickup, transfers, published };
}

// Crea un envío. Admite la lista de unidades (serial_numbers) o solo la cantidad
// (quantity): en ese caso el servidor elige las unidades que la cuenta tiene en custodia.
// En la MISMA transacción se registra la transferencia de custodia de cada unidad.
app.post("/shipments", requireRole("distribuidor", "fabricante"), async (req, res) => {
  // 2.7 (A1): el origen es SIEMPRE la cuenta que inicia sesión.
  const from_entity = req.auth.name;
  const requestedTo = typeof req.body.to_entity === "string" ? req.body.to_entity.trim() : "";
  const { batch_id } = req.body;
  const bySerials = Array.isArray(req.body.serial_numbers);
  const qty = req.body.quantity === undefined || req.body.quantity === null || req.body.quantity === "" ? null : Number(req.body.quantity);
  if (!bySerials && qty === null) return jsonError(res, 400, "Indica la cantidad (quantity) o las unidades (serial_numbers) que se envían");
  if (!bySerials && (!Number.isInteger(qty) || qty < 1 || qty > MAX_SERIALS)) return jsonError(res, 400, `La cantidad debe ser un número entero entre 1 y ${MAX_SERIALS}`);
  const requested = bySerials ? [...new Set(req.body.serial_numbers.map((x) => String(x).trim()).filter(Boolean))] : null;
  if (!batch_id) return jsonError(res, 400, "Selecciona el lote a enviar");
  if (!requestedTo) return jsonError(res, 400, "El destino es obligatorio");
  if (sameEntity(from_entity, requestedTo)) return jsonError(res, 400, "El origen y el destino no pueden ser iguales");
  if (bySerials && !requested.length) return jsonError(res, 400, "El envío debe incluir al menos una unidad");
  if (bySerials && requested.length > MAX_SERIALS) return jsonError(res, 400, `Un envío admite como máximo ${MAX_SERIALS} unidades`);
  if (typeof req.body.notes === "string" && req.body.notes.trim().length > MAX_NOTES) return jsonError(res, 400, `Las observaciones admiten como máximo ${MAX_NOTES} caracteres`);
  const notes = cleanNotes(req.body.notes);

  let batch, to, serialsFactory, serialsStock;
  try {
    batch = await loadBatch(req, batch_id);
    if (req.auth.role === "fabricante" && !sameEntity(batch.manufacturer, from_entity)) {
      throw new Rejection(403, "Solo el fabricante que registró el lote puede despacharlo");
    }
    to = await checkDestination(req, requestedTo);
    const known = await batchSerials(req, batch.id);
    if (bySerials) {
      checkSerialsBelong(known, requested);
      const cls = await classify(from_entity, requested);
      const problem = custodyProblem(req, cls);
      if (problem) throw problem;
      ({ fromFactory: serialsFactory, fromStock: serialsStock } = eligibleFor(req, batch, cls));
    } else {
      const active = [...known.entries()].filter(([, st]) => st !== "retirado").map(([s]) => s).sort();
      const cls = await classify(from_entity, active);
      const el = eligibleFor(req, batch, cls);
      const available = el.fromStock.length + el.fromFactory.length;
      if (qty > available) {
        throw new Rejection(409, available
          ? `Solo tienes ${available} unidad(es) disponibles de este lote para enviar${cls.pending.length ? ` (y ${cls.pending.length} en camino hacia ti, pendientes de confirmar)` : ""}.`
          : `No tienes unidades disponibles de este lote para enviar${cls.pending.length ? `: ${cls.pending.length} van en camino hacia ti y debes confirmar primero su recepción` : ""}.`, { available });
      }
      // Primero lo que ya está en su bodega; después lo que sigue en fábrica.
      serialsStock = el.fromStock.slice(0, qty);
      serialsFactory = el.fromFactory.slice(0, qty - serialsStock.length);
    }
  } catch (e) { return reject(res, e, "Error validando el envío"); }

  const client = await pool.connect();
  let out;
  try {
    await client.query("BEGIN");
    out = await registerMovement(client, req, { batch, to, serialsFactory, serialsStock, notes });
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    return fail(res, e, "Error registrando envío");
  }
  client.release();
  res.status(201).json({
    ...out.shipment, transfers: out.transfers.length,
    factory_pickup: out.pickup ? { id: out.pickup.id, from_entity: out.pickup.from_entity, units: out.pickup.units } : null,
  });
  for (const p of out.published) await publish("custody.transferred", p);
});

// 3.2: unidades que la cuenta puede enviar, por lote (para el formulario de envío).
app.get("/custody/mine", requireRole("distribuidor", "fabricante", "veterinaria"), async (req, res) => {
  try {
    const me = req.auth.name;
    const r = await callService(MANUFACTURING, "/batches", req, { timeout: 15000 });
    if (!r.ok) throw new Rejection(503, "No se pudieron consultar los lotes. Intenta de nuevo.");
    let batches = r.data || [];
    if (req.auth.role === "fabricante") batches = batches.filter((b) => sameEntity(b.manufacturer, me));
    const ids = batches.map((b) => b.id);
    const q = await pool.query(
      `with last as (
         select distinct on (ct.serial_number) ct.serial_number, s.batch_id, ct.to_entity, s.status
           from custody_transfers ct join shipments s on s.id = ct.shipment_id
          where s.batch_id = any($1::uuid[])
          order by ct.serial_number, ct.timestamp desc, ct.seq desc)
       select batch_id, count(*)::int as moved,
              count(*) filter (where lower(trim(to_entity)) = lower(trim($2)) and status = 'entregado')::int as held,
              count(*) filter (where lower(trim(to_entity)) = lower(trim($2)) and status <> 'entregado')::int as pending
         from last group by batch_id`, [ids, me]);
    const byBatch = new Map(q.rows.map((x) => [x.batch_id, x]));
    res.json(batches.map((b) => {
      const x = byBatch.get(b.id) || { moved: 0, held: 0, pending: 0 };
      const atFactory = b.status === "recall" ? 0 : Math.max(0, Number(b.serial_count || 0) - x.moved);
      const fromFactory = req.auth.role === "veterinaria" ? 0 : atFactory;
      return { batch_id: b.id, at_factory: atFactory, held: x.held, pending: x.pending, available: req.auth.role === "veterinaria" ? 0 : x.held + fromFactory };
    }));
  } catch (e) { reject(res, e, "Error consultando tu inventario"); }
});

// 2.7 (M1): cada rol ve solo los envíos que le corresponden.
//   master-admin: todos · admin (2.10): los que envió o recibió una entidad de su ámbito o
//   que son de lotes de sus fabricantes · distribuidor: los que envió o recibió ·
//   veterinaria: los que recibió · fabricante (3.2): los de sus propios lotes y los que envió.
async function adminFilter(req, query = {}) {
  let scope;
  try { scope = await getScope(req, query); }
  catch (e) { if (e instanceof ScopeError) throw new Rejection(e.status, e.message); throw e; }
  if (scope.all) return { sql: "true", params: [], scope };
  const r = await callService(MANUFACTURING, "/batches", req, { timeout: 15000 });
  if (!r.ok) throw new Rejection(503, "No se pudieron consultar los lotes. Intenta de nuevo.");
  const own = (r.data || []).filter((b) => scope.has(b.manufacturer)).map((b) => b.id);
  return { sql: "(lower(trim(from_entity)) = any($1::text[]) or lower(trim(to_entity)) = any($1::text[]) or batch_id = any($2::uuid[]))", params: [[...scope.names], own], scope };
}
async function ownBatchIds(req) {
  const r = await callService(MANUFACTURING, "/batches", req, { timeout: 15000 });
  if (!r.ok) throw new Rejection(503, "No se pudieron consultar tus lotes. Intenta de nuevo.");
  return (r.data || []).filter((b) => sameEntity(b.manufacturer, req.auth.name)).map((b) => b.id);
}
async function visibleFilter(req, query) {
  const { role, name } = req.auth;
  if (isAdmin(req)) return adminFilter(req, query);
  if (role === "distribuidor") return { sql: "(lower(trim(from_entity))=lower(trim($1)) or lower(trim(to_entity))=lower(trim($1)))", params: [name] };
  if (role === "veterinaria") return { sql: "lower(trim(to_entity))=lower(trim($1))", params: [name] };
  if (role === "fabricante") return { sql: "(batch_id = any($1::uuid[]) or lower(trim(from_entity))=lower(trim($2)))", params: [await ownBatchIds(req), name] };
  return { sql: "false", params: [] };
}
async function canSeeShipment(req, s) {
  const { role, name } = req.auth;
  if (isMaster(req)) return true;
  if (role === "admin") {
    const f = await adminFilter(req);
    return f.scope.has(s.from_entity) || f.scope.has(s.to_entity) || f.params[1].includes(s.batch_id);
  }
  if (role === "distribuidor") return sameEntity(s.from_entity, name) || sameEntity(s.to_entity, name);
  if (role === "veterinaria") return sameEntity(s.to_entity, name);
  if (role === "fabricante") return sameEntity(s.from_entity, name) || (await ownBatchIds(req)).includes(s.batch_id);
  return false;
}

app.get("/shipments", async (req, res) => {
  try {
    // 2.10: la administración puede acotar a un administrador o usuario (reportes) y por fechas.
    const f = await visibleFilter(req, isAdmin(req) ? { adminId: req.query.admin_id, userId: req.query.user_id } : {});
    const params = [...f.params];
    let where = `where ${f.sql}`;
    // 3.2: los retiros en fábrica registrados automáticamente solo se listan con ?kind=all
    // (así los reportes y contadores existentes siguen contando solo los envíos).
    if (req.query.kind !== "all") where += " and kind = 'envio'";
    if (req.query.batch_id) {
      if (!UUID_RE.test(String(req.query.batch_id))) return res.json([]);
      params.push(req.query.batch_id);
      where += ` and batch_id=$${params.length}`;
    }
    if (req.query.status) {
      if (!["en_transito", "entregado"].includes(String(req.query.status))) return jsonError(res, 400, "Estado no válido");
      params.push(String(req.query.status));
      where += ` and status=$${params.length}`;
    }
    for (const [q, op] of [["from", ">="], ["to", "<"]]) {
      if (!req.query[q]) continue;
      if (isNaN(new Date(String(req.query[q])))) return jsonError(res, 400, `Fecha "${q}" no válida`);
      params.push(String(req.query[q]));
      where += ` and shipped_at ${op} $${params.length}::timestamptz`;
    }
    // Antes el límite fijo de 500 recortaba los reportes sin avisar. La administración puede
    // pedir hasta 20.000 filas (los listados de pantalla siguen usando 500 por defecto).
    const max = isAdmin(req) ? 20000 : 500;
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 500, 1), max);
    res.json((await pool.query(`select * from shipments ${where} order by shipped_at desc limit ${limit}`, params)).rows);
  } catch (e) { reject(res, e, "Error consultando envíos"); }
});

// Confirma la recepción en destino. 3.2: la confirma quien RECIBE (distribuidor o
// veterinaria). Por compatibilidad, el remitente también puede marcarla, pero queda
// registrado que la marcó el remitente. Se guarda quién, cuándo y las observaciones.
app.patch("/shipments/:id/deliver", requireRole("distribuidor", "veterinaria", "fabricante"), async (req, res) => {
  try {
    if (!UUID_RE.test(req.params.id)) return jsonError(res, 404, "Envío no encontrado");
    if (typeof req.body.notes === "string" && req.body.notes.trim().length > MAX_NOTES) return jsonError(res, 400, `Las observaciones admiten como máximo ${MAX_NOTES} caracteres`);
    const current = (await pool.query("select * from shipments where id=$1", [req.params.id])).rows[0];
    if (!current) return jsonError(res, 404, "Envío no encontrado");
    const isReceiver = sameEntity(current.to_entity, req.auth.name);
    const isSender = sameEntity(current.from_entity, req.auth.name);
    if (!isReceiver && !isSender) {
      if (!(await canSeeShipment(req, current))) return jsonError(res, 404, "Envío no encontrado");
      return jsonError(res, 403, "Solo quien recibe el envío (o quien lo registró) puede confirmar la recepción");
    }
    const by = actor(req);
    const notes = cleanNotes(req.body.notes);
    const client = await pool.connect();
    let row;
    try {
      await client.query("BEGIN");
      const r = await client.query(
        `update shipments set status='entregado', delivered_at=now(), received_by_id=$2, received_by_name=$3, received_by_role=$4,
                reception_by=$5, reception_notes=$6
          where id=$1 and status<>'entregado' returning *`,
        [req.params.id, by.id, by.name, by.role, isReceiver ? "destinatario" : "remitente", notes]);
      if (!r.rowCount) { await client.query("ROLLBACK"); return jsonError(res, 409, "Este envío ya estaba marcado como entregado"); }
      row = r.rows[0];
      await insertEvent(client, row, "recepcion_confirmada", by, notes);
      await client.query("COMMIT");
    } catch (e) { await client.query("ROLLBACK").catch(() => {}); throw e; }
    finally { client.release(); }
    res.json(row);
    await publish("shipment.delivered", { shipment: row, batch_id: row.batch_id });
  } catch (e) { fail(res, e, "Error confirmando la entrega"); }
});

app.get("/shipments/:id", async (req, res) => {
  try {
    if (!UUID_RE.test(req.params.id)) return jsonError(res, 404, "Envío no encontrado");
    const s = (await pool.query("select * from shipments where id=$1", [req.params.id])).rows[0];
    if (!s || !(await canSeeShipment(req, s))) return jsonError(res, 404, "Envío no encontrado");
    const ev = await pool.query("select * from movement_events where shipment_id=$1 order by at, id", [s.id]);
    res.json({ ...s, events: ev.rows });
  } catch (e) { reject(res, e, "Error consultando el envío"); }
});

// Transferencia de una unidad dentro de un envío ya registrado. 2.7: el origen es la
// cuenta que inicia sesión y debe ser quien registró el envío; el destino es el del
// envío; la unidad debe pertenecer al lote del envío y estar bajo su custodia.
app.post("/custody-transfers", requireRole("distribuidor"), async (req, res) => {
  try {
    const { shipment_id } = req.body;
    const serial_number = typeof req.body.serial_number === "string" ? req.body.serial_number.trim() : "";
    if (!shipment_id || !serial_number) return jsonError(res, 400, "Datos incompletos");
    if (!UUID_RE.test(String(shipment_id))) return jsonError(res, 404, "Envío no encontrado");

    const shipment = (await pool.query("select * from shipments where id=$1", [shipment_id])).rows[0];
    if (!shipment) return jsonError(res, 404, "Envío no encontrado");
    if (!sameEntity(shipment.from_entity, req.auth.name) || shipment.kind !== "envio") return jsonError(res, 403, "Solo quien registró el envío puede agregarle unidades");
    let batch, cls;
    try {
      batch = await loadBatch(req, shipment.batch_id);
      checkSerialsBelong(await batchSerials(req, shipment.batch_id), [serial_number]);
      cls = await classify(req.auth.name, [serial_number]);
      const problem = custodyProblem(req, cls);
      if (problem) throw problem;
    } catch (e) { return reject(res, e, "Error validando la transferencia"); }

    const by = actor(req);
    const client = await pool.connect();
    const published = [];
    let transfer;
    try {
      await client.query("BEGIN");
      if (cls.factory.length) {
        const pickup = await insertShipment(client, {
          batch_id: batch.id, lot_number: batch.product_id || null, product_name: batch.product_name || null,
          from_entity: batch.manufacturer || UNKNOWN_MANUFACTURER, to_entity: by.name, units: 1, kind: "retiro_fabrica",
          from_role: "fabricante", to_role: "distribuidor", notes: "Retiro en fábrica registrado por el distribuidor (el fabricante no registró el despacho)",
          by, status: "entregado", offsetSecs: 0.001, delivered: true, received: by, reception_by: "destinatario",
        });
        published.push(...(await insertTransfers(client, pickup, [serial_number], by)).map((t) => ({ transfer: t, batch_id: batch.id })));
        await insertEvent(client, pickup, "envio_registrado", by, pickup.notes);
        await insertEvent(client, pickup, "recepcion_confirmada", by, "Recibida en fábrica por el distribuidor");
      }
      transfer = (await client.query(
        `insert into custody_transfers(id,shipment_id,serial_number,from_entity,to_entity,sent_by_id,sent_by_name,sent_by_role)
         values($1,$2,$3,$4,$5,$6,$7,$8) returning *`,
        [crypto.randomUUID(), shipment_id, serial_number, by.name, shipment.to_entity, by.id, by.name, by.role])).rows[0];
      const upd = (await client.query("update shipments set units = units + 1 where id=$1 returning *", [shipment_id])).rows[0];
      await client.query(
        `insert into movement_events(shipment_id,batch_id,event_type,actor_id,actor_name,actor_role,from_entity,to_entity,units,notes)
         values($1,$2,'unidad_agregada',$3,$4,$5,$6,$7,1,$8)`,
        [upd.id, upd.batch_id, by.id, by.name, by.role, upd.from_entity, upd.to_entity, `Unidad ${serial_number}`]);
      await client.query("COMMIT");
    } catch (e) { await client.query("ROLLBACK").catch(() => {}); throw e; }
    finally { client.release(); }
    published.push({ transfer, batch_id: shipment.batch_id });
    for (const p of published) await publish("custody.transferred", p);
    res.status(201).json(transfer);
  } catch (e) { fail(res, e, "Error registrando transferencia"); }
});

// =============================================================================
// 3.2 — Historial de movimientos (quién entregó qué a quién, cuándo y quién lo recibió)
// =============================================================================
// Acceso al historial de un lote:
//   - completo: Administrador; gerente cuyo ámbito incluye al fabricante del lote;
//     el fabricante dueño del lote.
//   - parcial: el resto de participantes (distribuidor, veterinaria, gerente cuyo ámbito
//     solo incluye a algún participante). Ven sus propios movimientos y el recorrido
//     ANTERIOR de las unidades que pasaron por ellos (de dónde vienen), pero no a quién
//     las entregó después el siguiente eslabón.
//   - sin movimientos propios: 404 (no se revela si el lote circula).
async function historyAccess(req, batch) {
  const { role, name } = req.auth;
  if (isMaster(req)) return { full: true };
  if (role === "admin") {
    let scope;
    try { scope = await getScope(req); }
    catch (e) { if (e instanceof ScopeError) throw new Rejection(e.status, e.message); throw e; }
    if (scope.all || scope.has(batch.manufacturer)) return { full: true };
    return { full: false, names: [...scope.names] };
  }
  if (role === "fabricante") {
    if (sameEntity(batch.manufacturer, name)) return { full: true };
    throw new Rejection(404, "No hay movimientos de este lote que puedas consultar");
  }
  if (["distribuidor", "veterinaria"].includes(role)) return { full: false, names: [String(name).trim().toLowerCase()] };
  throw new Rejection(403, "Tu rol no tiene acceso a esta información");
}

const party = (name, role, id = null) => ({ name: name || null, role: role || null, id: id ?? null });
// Movimiento con los responsables ya resueltos (también para los envíos anteriores a 3.2).
function movementView(s, extra = {}) {
  const legacy = !s.sent_by_name;
  const delivered = s.status === "entregado";
  return {
    id: s.id, batch_id: s.batch_id, kind: s.kind || "envio", lot_number: s.lot_number, product_name: s.product_name,
    from: party(s.from_entity, s.from_role), to: party(s.to_entity, s.to_role), units: Number(s.units || 0),
    sent_at: s.shipped_at, notes: s.notes || null,
    sent_by: legacy ? { ...party(s.from_entity, s.from_role), recorded: false } : { ...party(s.sent_by_name, s.sent_by_role, s.sent_by_id), recorded: true },
    status: s.status,
    reception: delivered
      ? { confirmed: true, at: s.delivered_at, as: s.reception_by || null, notes: s.reception_notes || null,
          by: s.received_by_name ? { ...party(s.received_by_name, s.received_by_role, s.received_by_id), recorded: true } : { name: null, role: null, id: null, recorded: false } }
      : { confirmed: false, at: null, as: null, notes: null, by: null },
    ...extra,
  };
}
const batchView = (b) => ({ id: b.id, lot_number: b.product_id, product_name: b.product_name, manufacturer: b.manufacturer || null,
  manufacture_date: b.manufacture_date, expiry_date: b.expiry_date, quantity: b.quantity, created_at: b.created_at, status: b.status });

const PARTICIPANT_SQL = `
  with t as (
    select ct.serial_number, ct.shipment_id, ct.from_entity, ct.to_entity, ct.timestamp, ct.seq
      from custody_transfers ct join shipments s on s.id = ct.shipment_id where s.batch_id = $1),
  lim as (
    select distinct on (serial_number) serial_number, timestamp, seq from t
     where lower(trim(from_entity)) = any($2::text[]) or lower(trim(to_entity)) = any($2::text[])
     order by serial_number, timestamp desc, seq desc)
  select t.shipment_id, count(*)::int as visible_units
    from t join lim on lim.serial_number = t.serial_number and (t.timestamp, t.seq) <= (lim.timestamp, lim.seq)
   group by t.shipment_id`;

app.get("/history/batch/:batch_id", async (req, res) => {
  try {
    if (!UUID_RE.test(req.params.batch_id)) return jsonError(res, 404, "Lote no encontrado");
    const batch = await fetchBatch(req, req.params.batch_id);
    const access = await historyAccess(req, batch);
    let rows, visible = null;
    if (access.full) {
      rows = (await pool.query("select * from shipments where batch_id=$1 order by shipped_at, id", [batch.id])).rows;
    } else {
      const v = await pool.query(PARTICIPANT_SQL, [batch.id, access.names]);
      visible = new Map(v.rows.map((x) => [x.shipment_id, x.visible_units]));
      rows = visible.size ? (await pool.query("select * from shipments where id = any($1::uuid[]) order by shipped_at, id", [[...visible.keys()]])).rows : [];
      if (!rows.length) return jsonError(res, 404, "No hay movimientos de este lote que puedas consultar");
    }
    const ev = rows.length ? (await pool.query("select * from movement_events where shipment_id = any($1::uuid[]) order by at, id", [rows.map((r) => r.id)])).rows : [];
    const evBy = new Map();
    ev.forEach((e) => { if (!evBy.has(e.shipment_id)) evBy.set(e.shipment_id, []); evBy.get(e.shipment_id).push(e); });
    const movements = rows.map((s) => movementView(s, {
      events: (evBy.get(s.id) || []).map((e) => ({ type: e.event_type, at: e.at, by: party(e.actor_name, e.actor_role, e.actor_id), units: e.units, notes: e.notes })),
      ...(visible ? { your_units: visible.get(s.id) || 0 } : {}),
    }));

    let custody = null;
    if (access.full) {
      const known = await batchSerials(req, batch.id);
      const h = await pool.query(
        `with last as (
           select distinct on (ct.serial_number) ct.serial_number, ct.to_entity, s.status, s.to_role
             from custody_transfers ct join shipments s on s.id = ct.shipment_id
            where s.batch_id = $1 order by ct.serial_number, ct.timestamp desc, ct.seq desc)
         select to_entity, max(to_role) as role, status, count(*)::int as units from last group by to_entity, status order by units desc`, [batch.id]);
      const moved = h.rows.reduce((a, x) => a + x.units, 0);
      custody = {
        total_units: known.size,
        at_factory: Math.max(0, known.size - moved),
        holders: h.rows.map((x) => ({ entity: x.to_entity, role: x.role, status: x.status, units: x.units })),
      };
    }
    res.json({ batch: batchView(batch), visibility: access.full ? "completa" : "parcial", movements, custody });
  } catch (e) { reject(res, e, "Error consultando el historial del lote"); }
});

app.get("/history/serial/:serial", async (req, res) => {
  try {
    const serial = String(req.params.serial || "").trim();
    if (!serial || serial.length > 100) return jsonError(res, 404, "Unidad no registrada");
    let sr;
    try { sr = await callService(MANUFACTURING, `/serial-numbers/${encodeURIComponent(serial)}`, req); }
    catch { throw new Rejection(503, "No se pudo consultar la unidad con Manufacturing. Intenta de nuevo."); }
    if (sr.status === 404) return jsonError(res, 404, "Unidad no registrada");
    if (!sr.ok) throw new Rejection(503, "No se pudo consultar la unidad con Manufacturing. Intenta de nuevo.");
    const batch = await fetchBatch(req, sr.data.batch_id);
    const access = await historyAccess(req, batch);
    let rows = (await pool.query(
      `select ct.id as transfer_id, ct.timestamp, ct.seq, ct.from_entity as t_from, ct.to_entity as t_to,
              ct.sent_by_id as t_by_id, ct.sent_by_name as t_by_name, ct.sent_by_role as t_by_role, s.*
         from custody_transfers ct join shipments s on s.id = ct.shipment_id
        where ct.serial_number = $1 order by ct.timestamp, ct.seq`, [serial])).rows;
    if (!access.full) {
      const names = new Set(access.names);
      let last = -1;
      rows.forEach((r, i) => { if (names.has(String(r.t_from).trim().toLowerCase()) || names.has(String(r.t_to).trim().toLowerCase())) last = i; });
      if (last < 0) return jsonError(res, 404, "No hay movimientos de esta unidad que puedas consultar");
      rows = rows.slice(0, last + 1);
    }
    const transfers = rows.map((r) => movementView(r, {
      transfer_id: r.transfer_id, at: r.timestamp,
      // Si la unidad se agregó después a un envío, quién la agregó es quien la transfirió.
      ...(r.t_by_name ? { sent_by: { ...party(r.t_by_name, r.t_by_role, r.t_by_id), recorded: true } } : {}),
    }));
    let current = null;
    if (access.full) {
      const l = transfers[transfers.length - 1];
      current = l ? { entity: l.to.name, role: l.to.role, in_transit: l.status !== "entregado", since: l.sent_at }
        : { entity: batch.manufacturer || UNKNOWN_MANUFACTURER, role: "fabricante", in_transit: false, since: batch.created_at, at_factory: true };
    }
    res.json({ serial, unit_status: sr.data.status, batch: batchView(batch), visibility: access.full ? "completa" : "parcial", transfers, current });
  } catch (e) { reject(res, e, "Error consultando el historial de la unidad"); }
});

app.get("/custody-transfers/serial/:serial", async (req, res) => {
  const r = await pool.query(
    "select * from custody_transfers where serial_number=$1 order by timestamp, seq", [req.params.serial]);
  res.json(r.rows);
});

installErrorHandlers(app);
app.listen(PORT, () => console.log(`Distribution Service http://localhost:${PORT}`));
