import "dotenv/config";
import express from "express";
import crypto from "crypto";
import { pool, initDb, publish, jsonError, fail, bodyDefault, installErrorHandlers,
  authenticate, auditTrail, requireRole, isAdmin, sameEntity, callService, withScheme, AUTH_URL } from "./common.js";

const app = express();
const PORT = process.env.PORT || 3003;
const MANUFACTURING = withScheme(process.env.MANUFACTURING_URL || "localhost:3002");
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const MAX_SERIALS = 100000;

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

app.get("/", (_, res) => res.json({ service: "distribution-service", status: "ok" }));
app.get("/health", async (_, res) => {
  try { await pool.query("select 1"); res.json({ status: "ok" }); }
  catch { res.status(503).json({ status: "down" }); }
});

// 2.7: todas las rutas (salvo / y /health) exigen una sesión válida (C1).
// 2.8: registro de auditoría (va antes de authenticate para anotar también los 401).
app.use(auditTrail("distribution", [
  ["POST", /^\/shipments\/?$/, "shipment.created", (b, r) => ({ target: r.to_entity || b.to_entity, message: `Registró un envío de ${r.transfers ?? (b.serial_numbers || []).length} unidades hacia ${r.to_entity || b.to_entity || "?"}` })],
  ["PATCH", /^\/shipments\/[^/]+\/deliver\/?$/, "shipment.delivered", (b, r) => ({ target: r.to_entity, message: `Confirmó la entrega en ${r.to_entity || "destino"}` })],
  ["POST", /^\/custody-transfers\/?$/, "custody.transferred", (b, r) => ({ target: r.serial_number || b.serial_number, message: `Transfirió la unidad ${r.serial_number || b.serial_number || ""}` })],
]));
app.use(authenticate);

// -----------------------------------------------------------------------------
// Validaciones de custodia (2.7, C3 y A1)
// -----------------------------------------------------------------------------
class Rejection extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

// El lote debe existir en Manufacturing, no estar en recall ni vencido.
async function loadBatch(req, batchId) {
  if (!UUID_RE.test(String(batchId || ""))) throw new Rejection(400, "Selecciona un lote válido");
  let r;
  try { r = await callService(MANUFACTURING, `/batches/${batchId}`, req); }
  catch { throw new Rejection(503, "No se pudo validar el lote con Manufacturing. Intenta de nuevo."); }
  if (r.status === 404) throw new Rejection(404, "El lote no existe");
  if (!r.ok) throw new Rejection(503, "No se pudo validar el lote con Manufacturing. Intenta de nuevo.");
  const batch = r.data;
  if (batch.status === "recall") throw new Rejection(409, "El lote está en recall: no se puede distribuir");
  if (String(batch.expiry_date) < new Date().toISOString().slice(0, 10)) throw new Rejection(409, "El lote está vencido: no se puede distribuir");
  return batch;
}

// Cada serial debe existir, pertenecer al lote y no estar retirado. Antes (2.6)
// se aceptaba cualquier texto y Traceability lo daba por auténtico (C3).
async function checkSerialsBelong(req, batchId, serials) {
  let r;
  try { r = await callService(MANUFACTURING, `/serial-numbers/batch/${batchId}?limit=${MAX_SERIALS}`, req, { timeout: 15000 }); }
  catch { throw new Rejection(503, "No se pudieron validar las unidades con Manufacturing. Intenta de nuevo."); }
  if (!r.ok) throw new Rejection(503, "No se pudieron validar las unidades con Manufacturing. Intenta de nuevo.");
  const known = new Map((r.data || []).map((s) => [s.serial_code, s.status]));
  const unknown = serials.filter((s) => !known.has(s));
  if (unknown.length) {
    throw new Rejection(409, `${unknown.length} número(s) de serie no pertenecen a este lote o no están registrados (p. ej. ${unknown[0]}). Solo se pueden enviar unidades fabricadas en VetChain.`);
  }
  const retired = serials.filter((s) => known.get(s) === "retirado");
  if (retired.length) throw new Rejection(409, `${retired.length} unidad(es) están retiradas (recall) y no se pueden enviar`);
}

// La cuenta que envía debe tener la custodia: o la unidad nunca salió del
// fabricante, o la última transferencia fue hacia esta entidad (A1).
async function checkCustody(entity, serials) {
  const r = await pool.query(
    `select distinct on (serial_number) serial_number, to_entity
       from custody_transfers where serial_number = any($1::text[])
      order by serial_number, timestamp desc`, [serials]);
  const notMine = r.rows.filter((t) => !sameEntity(t.to_entity, entity));
  if (notMine.length) {
    throw new Rejection(409, `${notMine.length} unidad(es) no están bajo tu custodia (la tiene ${notMine[0].to_entity}).`);
  }
}

// El destino debe ser una cuenta activa registrada (distribuidor o veterinaria).
async function checkDestination(req, toEntity) {
  let r;
  try { r = await callService(AUTH_URL, "/entities", req); }
  catch { throw new Rejection(503, "No se pudo validar el destino. Intenta de nuevo."); }
  if (!r.ok) throw new Rejection(503, "No se pudo validar el destino. Intenta de nuevo.");
  const match = (r.data || []).find((e) => sameEntity(e.name, toEntity));
  if (!match) throw new Rejection(400, "El destino debe ser una cuenta registrada y activa (distribuidor o veterinaria)");
  return match.name;
}

const reject = (res, e, fallback) => (e instanceof Rejection ? jsonError(res, e.status, e.message) : fail(res, e, fallback));

// Crea un envío. Si se envían serial_numbers, en la MISMA operación se registra la
// transferencia de custodia de cada unidad (antes el frontend solo transfería la
// primera unidad del lote).
app.post("/shipments", requireRole("distribuidor"), async (req, res) => {
  // 2.7 (A1): el origen es SIEMPRE la cuenta que inicia sesión.
  const from_entity = req.auth.name;
  const requestedTo = typeof req.body.to_entity === "string" ? req.body.to_entity.trim() : "";
  const { batch_id } = req.body;
  if (!Array.isArray(req.body.serial_numbers)) return jsonError(res, 400, "Indica las unidades (serial_numbers) que se envían");
  const serials = [...new Set(req.body.serial_numbers.map((x) => String(x).trim()).filter(Boolean))];
  if (!batch_id) return jsonError(res, 400, "Selecciona el lote a enviar");
  if (!requestedTo) return jsonError(res, 400, "El destino es obligatorio");
  if (sameEntity(from_entity, requestedTo)) return jsonError(res, 400, "El origen y el destino no pueden ser iguales");
  if (!serials.length) return jsonError(res, 400, "El envío debe incluir al menos una unidad");
  if (serials.length > MAX_SERIALS) return jsonError(res, 400, `Un envío admite como máximo ${MAX_SERIALS} unidades`);

  let to_entity;
  try {
    await loadBatch(req, batch_id);
    to_entity = await checkDestination(req, requestedTo);
    await checkSerialsBelong(req, batch_id, serials);
    await checkCustody(from_entity, serials);
  } catch (e) { return reject(res, e, "Error validando el envío"); }

  const client = await pool.connect();
  let shipment, transfers = [];
  try {
    await client.query("BEGIN");
    shipment = (await client.query(
      "insert into shipments(id,batch_id,from_entity,to_entity,units) values($1,$2,$3,$4,$5) returning *",
      [crypto.randomUUID(), batch_id, from_entity, to_entity, serials.length])).rows[0];
    if (serials.length) {
      transfers = (await client.query(
        `insert into custody_transfers(id,shipment_id,serial_number,from_entity,to_entity)
         select gen_random_uuid(), $1, s, $2, $3 from unnest($4::text[]) as s returning *`,
        [shipment.id, from_entity, to_entity, serials])).rows;
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    return fail(res, e, "Error registrando envío");
  }
  client.release();
  res.status(201).json({ ...shipment, transfers: transfers.length });
  for (const t of transfers) await publish("custody.transferred", { transfer: t, batch_id: shipment.batch_id });
});

// 2.7 (M1): cada rol ve solo los envíos que le corresponden.
//   admin: todos · distribuidor: los que envió o recibió · veterinaria: los que recibió
//   fabricante: los de sus propios lotes.
async function visibleFilter(req) {
  const { role, name } = req.auth;
  if (role === "admin") return { sql: "true", params: [] };
  if (role === "distribuidor") return { sql: "(lower(trim(from_entity))=lower(trim($1)) or lower(trim(to_entity))=lower(trim($1)))", params: [name] };
  if (role === "veterinaria") return { sql: "lower(trim(to_entity))=lower(trim($1))", params: [name] };
  if (role === "fabricante") {
    const r = await callService(MANUFACTURING, "/batches", req, { timeout: 15000 });
    if (!r.ok) throw new Rejection(503, "No se pudieron consultar tus lotes. Intenta de nuevo.");
    const own = (r.data || []).filter((b) => sameEntity(b.manufacturer, name)).map((b) => b.id);
    return { sql: "batch_id = any($1::uuid[])", params: [own] };
  }
  return { sql: "false", params: [] };
}
const canSee = (req, s) => {
  const { role, name } = req.auth;
  if (role === "admin") return true;
  if (role === "distribuidor") return sameEntity(s.from_entity, name) || sameEntity(s.to_entity, name);
  if (role === "veterinaria") return sameEntity(s.to_entity, name);
  return false; // fabricante: se resuelve con visibleFilter
};

app.get("/shipments", async (req, res) => {
  try {
    const f = await visibleFilter(req);
    const params = [...f.params];
    let where = `where ${f.sql}`;
    if (req.query.batch_id) {
      if (!UUID_RE.test(String(req.query.batch_id))) return res.json([]);
      params.push(req.query.batch_id);
      where += ` and batch_id=$${params.length}`;
    }
    res.json((await pool.query(`select * from shipments ${where} order by shipped_at desc limit 500`, params)).rows);
  } catch (e) { reject(res, e, "Error consultando envíos"); }
});

// Confirma la entrega en destino. 2.7 (A1): solo el distribuidor que envió o el
// que recibe pueden confirmarla.
app.patch("/shipments/:id/deliver", requireRole("distribuidor"), async (req, res) => {
  try {
    if (!UUID_RE.test(req.params.id)) return jsonError(res, 404, "Envío no encontrado");
    const current = (await pool.query("select * from shipments where id=$1", [req.params.id])).rows[0];
    if (!current) return jsonError(res, 404, "Envío no encontrado");
    if (!sameEntity(current.from_entity, req.auth.name) && !sameEntity(current.to_entity, req.auth.name)) {
      return jsonError(res, 403, "Solo el distribuidor que registró el envío (o quien lo recibe) puede confirmar la entrega");
    }
    const r = await pool.query(
      `update shipments set status='entregado', delivered_at=now()
        where id=$1 and status<>'entregado' returning *`, [req.params.id]);
    if (!r.rowCount) return jsonError(res, 409, "Este envío ya estaba marcado como entregado");
    res.json(r.rows[0]);
    await publish("shipment.delivered", { shipment: r.rows[0], batch_id: r.rows[0].batch_id });
  } catch (e) { fail(res, e, "Error confirmando la entrega"); }
});

app.get("/shipments/:id", async (req, res) => {
  try {
    if (!UUID_RE.test(req.params.id)) return jsonError(res, 404, "Envío no encontrado");
    const r = await pool.query("select * from shipments where id=$1", [req.params.id]);
    const s = r.rows[0];
    if (!s) return jsonError(res, 404, "Envío no encontrado");
    if (req.auth.role === "fabricante") {
      const f = await visibleFilter(req);
      if (!f.params[0].includes(s.batch_id)) return jsonError(res, 404, "Envío no encontrado");
    } else if (!canSee(req, s)) return jsonError(res, 404, "Envío no encontrado");
    res.json(s);
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
    if (!sameEntity(shipment.from_entity, req.auth.name)) return jsonError(res, 403, "Solo quien registró el envío puede agregarle unidades");
    const from_entity = req.auth.name, to_entity = shipment.to_entity;
    try {
      await loadBatch(req, shipment.batch_id);
      await checkSerialsBelong(req, shipment.batch_id, [serial_number]);
      await checkCustody(from_entity, [serial_number]);
    } catch (e) { return reject(res, e, "Error validando la transferencia"); }

    const r = await pool.query(
      `insert into custody_transfers(id,shipment_id,serial_number,from_entity,to_entity)
       values($1,$2,$3,$4,$5) returning *`,
      [crypto.randomUUID(), shipment_id, serial_number, from_entity, to_entity]
    );
    await publish("custody.transferred", { transfer: r.rows[0], batch_id: shipment.batch_id });
    res.status(201).json(r.rows[0]);
  } catch (e) { fail(res, e, "Error registrando transferencia"); }
});

app.get("/custody-transfers/serial/:serial", async (req, res) => {
  const r = await pool.query(
    "select * from custody_transfers where serial_number=$1 order by timestamp", [req.params.serial]);
  res.json(r.rows);
});

installErrorHandlers(app);
app.listen(PORT, () => console.log(`Distribution Service http://localhost:${PORT}`));
