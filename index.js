import "dotenv/config";
import express from "express";
import crypto from "crypto";
import { pool, initDb, publish, jsonError, fail, bodyDefault, installErrorHandlers } from "./common.js";

const app = express();
const PORT = process.env.PORT || 3003;

app.use(express.json());
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

// Crea un envío. Si se envían serial_numbers, en la MISMA operación se registra la
// transferencia de custodia de cada unidad (antes el frontend solo transfería la
// primera unidad del lote).
app.post("/shipments", async (req, res) => {
  const from_entity = String(req.body.from_entity || "").trim();
  const to_entity = String(req.body.to_entity || "").trim();
  const { batch_id } = req.body;
  const serials = Array.isArray(req.body.serial_numbers)
    ? [...new Set(req.body.serial_numbers.map((x) => String(x).trim()).filter(Boolean))]
    : [];
  if (!batch_id) return jsonError(res, 400, "Selecciona el lote a enviar");
  if (!from_entity || !to_entity) return jsonError(res, 400, "Origen y destino son obligatorios");
  if (from_entity.toLowerCase() === to_entity.toLowerCase()) return jsonError(res, 400, "El origen y el destino no pueden ser iguales");

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

app.get("/shipments", async (req, res) => {
  const params = [];
  let where = "";
  if (req.query.batch_id) { params.push(req.query.batch_id); where = "where batch_id=$1"; }
  try {
    res.json((await pool.query(`select * from shipments ${where} order by shipped_at desc limit 500`, params)).rows);
  } catch (e) { fail(res, e, "Error consultando envíos"); }
});

// Confirma la entrega en destino.
app.patch("/shipments/:id/deliver", async (req, res) => {
  try {
    const r = await pool.query(
      `update shipments set status='entregado', delivered_at=now()
        where id=$1 and status<>'entregado' returning *`, [req.params.id]);
    if (!r.rowCount) {
      const exists = await pool.query("select status from shipments where id=$1", [req.params.id]);
      if (!exists.rowCount) return jsonError(res, 404, "Envío no encontrado");
      return jsonError(res, 409, "Este envío ya estaba marcado como entregado");
    }
    res.json(r.rows[0]);
    await publish("shipment.delivered", { shipment: r.rows[0], batch_id: r.rows[0].batch_id });
  } catch (e) { fail(res, e, "Error confirmando la entrega"); }
});

app.get("/shipments/:id", async (req, res) => {
  const r = await pool.query("select * from shipments where id=$1", [req.params.id]);
  if (!r.rowCount) return jsonError(res, 404, "Envío no encontrado");
  res.json(r.rows[0]);
});

app.post("/custody-transfers", async (req, res) => {
  try {
    const { shipment_id, serial_number, from_entity, to_entity } = req.body;
    if (!shipment_id || !serial_number || !from_entity || !to_entity) return jsonError(res, 400, "Datos incompletos");

    const shipment = (await pool.query("select id,batch_id from shipments where id=$1", [shipment_id])).rows[0];
    if (!shipment) return jsonError(res, 404, "Envío no encontrado");

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
