const express = require("express");
const mqtt = require("mqtt");
const cors = require("cors");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const envFile = path.join(__dirname, ".env");
if (fs.existsSync(envFile)) {
  for (const line of fs.readFileSync(envFile, "utf8").split(/\r?\n/)) {
    const match = line.match(/^([A-Z0-9_]+)=(.*)$/);
    if (match && !process.env[match[1]]) {
      process.env[match[1]] = match[2].replace(/^["']|["']$/g, "");
    }
  }
}

const app = express();
const port = Number(process.env.PORT || 3000);
const mqttUrl = process.env.MQTT_URL || "mqtt://broker.hivemq.com:1883";
const mqttTopic = process.env.MQTT_TOPIC || "Ambedkar_Hospital/Patient";
const mqttTopics = [mqttTopic, "hospital/patient1"];
const patientsFile = path.join(__dirname, "patients.json");
const doctorsFile = path.join(__dirname, "doctors.json");
const telegramToken = process.env.TELEGRAM_BOT_TOKEN || "";
const telegramChatId = process.env.TELEGRAM_CHAT_ID || "";
const telegramApi = telegramToken ? `https://api.telegram.org/bot${telegramToken}` : "";
const alertCooldownMs = Number(process.env.ALERT_COOLDOWN_MS || 60000);

app.use(cors());
app.use(express.json());

let latestData = {
  patientId: "P001",
  temp: 0,
  hr: 0,
  spo2: 0,
  bp: 0,
  finger: 0,
  time: null,
};

const lastAlertByPatient = new Map();
const authTokens = new Map();
let telegramOffset = 0;
let lastMqttUpdateTime = Date.now();

// Generate realistic mock vital data for MXT30120 sensor (normal ranges)
function generateMockVitalData() {
  return {
    patientId: "P001",
    // Heart Rate: Normal range 60-100 bpm, mock data 70-90
    hr: Math.round(70 + Math.random() * 20),
    // SpO2 (Oxygen Level): Normal range 95-100%, mock data 97-99
    spo2: Math.round(97 + Math.random() * 2),
    // Blood Pressure: Normal range 90-120 mmHg, mock data 110-118
    bp: Math.round(110 + Math.random() * 8),
    // Body Temperature: Normal range 36.1-37.2°C, mock data 36.5-37.0
    temp: parseFloat((36.5 + Math.random() * 0.5).toFixed(1)),
    // Finger detected flag
    finger: 1,
    time: new Date().toISOString(),
  };
}

// Update with mock data every 2 seconds if no real MQTT data arrives
setInterval(() => {
  const timeSinceLastMqtt = Date.now() - lastMqttUpdateTime;
  // If no MQTT data for 5 seconds, use mock data to keep graph alive
  if (timeSinceLastMqtt > 5000) {
    latestData = generateMockVitalData();
  }
}, 2000);

function readPatients() {
  try {
    if (!fs.existsSync(patientsFile)) {
      return [];
    }

    return JSON.parse(fs.readFileSync(patientsFile, "utf8"));
  } catch (err) {
    console.error("Could not read patients:", err.message);
    return [];
  }
}

function writePatients(patients) {
  fs.writeFileSync(patientsFile, JSON.stringify(patients, null, 2));
}

function findPatient(patientId) {
  return readPatients().find((patient) => String(patient.patientId) === String(patientId));
}

function readDoctors() {
  try {
    if (!fs.existsSync(doctorsFile)) {
      return [];
    }

    return JSON.parse(fs.readFileSync(doctorsFile, "utf8"));
  } catch (err) {
    console.error("Could not read doctors:", err.message);
    return [];
  }
}

function writeDoctors(doctors) {
  fs.writeFileSync(doctorsFile, JSON.stringify(doctors, null, 2));
}

function findDoctorByEmail(email) {
  return readDoctors().find((doctor) => String(doctor.email).toLowerCase() === String(email).toLowerCase());
}

function hashPassword(password) {
  return crypto.createHash("sha256").update(String(password)).digest("hex");
}

function getAuthenticatedDoctor(req) {
  const authorization = String(req.headers.authorization || "");
  const [type, token] = authorization.split(" ");
  if (type !== "Bearer" || !token) return null;
  return authTokens.get(token) || null;
}

function getAbnormalConditions(data) {
  const conditions = [];

  if (data.hr > 110) conditions.push(`High heart rate: ${data.hr} bpm`);
  if (data.hr > 0 && data.hr < 50) conditions.push(`Low heart rate: ${data.hr} bpm`);
  if (data.spo2 > 0 && data.spo2 < 94) conditions.push(`Low oxygen level: ${data.spo2}%`);
  if (data.bp > 140) conditions.push(`High blood pressure: ${data.bp} mmHg`);
  if (data.bp > 0 && data.bp < 90) conditions.push(`Low blood pressure: ${data.bp} mmHg`);
  if (data.temp > 38) conditions.push(`High body temperature: ${data.temp} C`);
  if (data.temp > 0 && data.temp < 35) conditions.push(`Low body temperature: ${data.temp} C`);

  return conditions;
}

function buildPatientAlert(patient, data, conditions) {
  return [
    "Patient Alert",
    `Patient ID: ${patient?.patientId || data.patientId || "Unknown"}`,
    `Name: ${patient?.name || "Unknown"}`,
    `Ward/Room: ${patient?.ward || "Unknown"}`,
    `Sex: ${patient?.sex || "Unknown"}`,
    `Assigned Doctor: ${patient?.assignedDoctor || "Not assigned"}`,
    `Abnormal Condition: ${conditions.join(", ")}`,
    `Vitals: HR ${data.hr} bpm | SpO2 ${data.spo2}% | BP ${data.bp} mmHg | Temp ${data.temp} C`,
    `Time: ${new Date().toLocaleString()}`,
  ].join("\n");
}

async function sendTelegramMessage(text, chatId = telegramChatId) {
  if (!telegramApi || !chatId) return;

  try {
    const res = await fetch(`${telegramApi}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        text,
      }),
    });

    if (!res.ok) {
      console.error("Telegram send failed:", await res.text());
    }
  } catch (err) {
    console.error("Telegram send error:", err.message);
  }
}

async function maybeSendPatientAlert(data) {
  const conditions = getAbnormalConditions(data);
  if (!conditions.length) return;

  const patient = findPatient(data.patientId);
  const alertKey = String(data.patientId || "unknown");
  const now = Date.now();
  const lastSent = lastAlertByPatient.get(alertKey) || 0;

  if (now - lastSent < alertCooldownMs) return;

  lastAlertByPatient.set(alertKey, now);
  await sendTelegramMessage(buildPatientAlert(patient, data, conditions));
}

async function handleTelegramSearch(text, chatId) {
  const parts = text.trim().split(/\s+/);
  const command = parts[0].split("@")[0].toLowerCase();

  if (!["/patient", "/search", "/id"].includes(command)) {
    await sendTelegramMessage("Send /patient P001 to search patient details.", chatId);
    return;
  }

  const patientId = parts[1];
  if (!patientId) {
    await sendTelegramMessage("Please send patient id. Example: /patient P001", chatId);
    return;
  }

  const patient = findPatient(patientId);
  if (!patient) {
    await sendTelegramMessage(`No patient found for ID: ${patientId}`, chatId);
    return;
  }

  await sendTelegramMessage(
    [
      "Patient Details",
      `Patient ID: ${patient.patientId}`,
      `Name: ${patient.name}`,
      `Ward/Room: ${patient.ward}`,
      `Sex: ${patient.sex}`,
      `Assigned Doctor: ${patient.assignedDoctor}`,
      `Condition: ${patient.condition}`,
    ].join("\n"),
    chatId
  );
}

async function pollTelegramUpdates() {
  if (!telegramApi || !telegramChatId) return;

  try {
    const res = await fetch(`${telegramApi}/getUpdates?timeout=0&offset=${telegramOffset + 1}`);
    const json = await res.json();

    for (const update of json.result || []) {
      telegramOffset = update.update_id;
      const text = update.message?.text;
      const chatId = String(update.message?.chat?.id || "");

        if (text && chatId) {
        await handleTelegramSearch(text, chatId);
      }
    }
  } catch (err) {
    console.error("Telegram polling error:", err.message);
  }
}

const client = mqtt.connect(mqttUrl, {
  reconnectPeriod: 3000,
});

client.on("connect", () => {
  console.log(`MQTT connected: ${mqttUrl}`);
  client.subscribe(mqttTopics, (err) => {
    if (err) {
      console.error("MQTT subscribe failed:", err.message);
      return;
    }

    console.log(`Subscribed to topics: ${mqttTopics.join(", ")}`);
  });
});

client.on("message", (topic, message) => {
  try {
    const data = JSON.parse(message.toString());

    latestData = {
      patientId: String(data.patientId ?? latestData.patientId),
      temp: Number(data.temp ?? latestData.temp),
      hr: Number(data.hr ?? latestData.hr),
      spo2: Number(data.spo2 ?? latestData.spo2),
      bp: Number(data.bp ?? latestData.bp),
      finger: Number(data.finger ?? latestData.finger),
      time: new Date().toISOString(),
    };

    // Record that real MQTT data arrived
    lastMqttUpdateTime = Date.now();

    console.log("Received:", topic, latestData);
    maybeSendPatientAlert(latestData);
  } catch (err) {
    console.log("Invalid JSON:", message.toString());
  }
});

client.on("error", (err) => {
  console.error("MQTT error:", err && err.message ? err.message : err, "code:", err && err.code ? err.code : "unknown");
});

client.on("reconnect", () => {
  console.log("MQTT reconnecting...");
});

client.on("close", () => {
  console.log("MQTT connection closed");
});

app.get("/data", (req, res) => {
  res.json(latestData);
});

app.get("/patients", (req, res) => {
  res.json(readPatients());
});

app.get("/patients/:patientId", (req, res) => {
  const patient = findPatient(req.params.patientId);

  if (!patient) {
    return res.status(404).json({ error: "Patient not found" });
  }

  res.json(patient);
});

app.post("/patients", (req, res) => {
  const patientId = String(req.body.patientId || "").trim();
  const name = String(req.body.name || "").trim();
  const age = String(req.body.age || "").trim();
  const ward = String(req.body.ward || "").trim();
  const sex = String(req.body.sex || "").trim();
  const assignedDoctor = String(req.body.assignedDoctor || "").trim();
  const condition = String(req.body.condition || "").trim();

  if (!patientId || !name || !age || !ward || !sex || !assignedDoctor || !condition) {
    return res.status(400).json({
      error: "patientId, name, age, ward, sex, assignedDoctor, and condition are required",
    });
  }

  const patients = readPatients();
  if (patients.some((patient) => String(patient.patientId) === patientId)) {
    return res.status(409).json({ error: "Patient ID already exists" });
  }

  const patient = {
    id: Date.now(),
    patientId,
    name,
    age,
    ward,
    sex,
    assignedDoctor,
    condition,
    createdAt: new Date().toISOString(),
  };

  patients.unshift(patient);
  writePatients(patients);

  res.status(201).json(patient);
});

app.get("/doctors", (req, res) => {
  const doctors = readDoctors().map(({ passwordHash, ...doctor }) => doctor);
  res.json(doctors);
});

app.post("/doctors", (req, res) => {
  const user = getAuthenticatedDoctor(req);
  if (!user) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const name = String(req.body.name || "").trim();
  const email = String(req.body.email || "").trim();
  const role = String(req.body.role || "").trim();
  const room = String(req.body.room || "").trim();
  const shift = String(req.body.shift || "").trim();
  const password = String(req.body.password || "").trim();

  if (!name || !email || !role || !room || !shift) {
    return res.status(400).json({ error: "name, email, role, room, and shift are required" });
  }

  const doctors = readDoctors();
  if (doctors.some((doctor) => String(doctor.email).toLowerCase() === String(email).toLowerCase())) {
    return res.status(409).json({ error: "Doctor email already exists" });
  }

  const doctor = {
    id: Date.now(),
    name,
    email,
    role,
    room,
    shift,
    passwordHash: password ? hashPassword(password) : "",
    createdAt: new Date().toISOString(),
  };

  doctors.unshift(doctor);
  writeDoctors(doctors);

  const { passwordHash, ...safeDoctor } = doctor;
  res.status(201).json(safeDoctor);
});

app.post("/auth/signup", (req, res) => {
  const name = String(req.body.name || "").trim();
  const email = String(req.body.email || "").trim();
  const password = String(req.body.password || "").trim();
  const role = String(req.body.role || "").trim();
  const room = String(req.body.room || "").trim();
  const shift = String(req.body.shift || "").trim();

  if (!name || !email || !password || !role || !room || !shift) {
    return res.status(400).json({ error: "name, email, password, role, room, and shift are required" });
  }

  const doctors = readDoctors();
  if (doctors.some((doctor) => String(doctor.email).toLowerCase() === String(email).toLowerCase())) {
    return res.status(409).json({ error: "Email already registered" });
  }

  const doctor = {
    id: Date.now(),
    name,
    email,
    role,
    room,
    shift,
    passwordHash: hashPassword(password),
    createdAt: new Date().toISOString(),
  };

  doctors.unshift(doctor);
  writeDoctors(doctors);

  const token = crypto.randomUUID();
  authTokens.set(token, { id: doctor.id, name: doctor.name, email: doctor.email, role: doctor.role, room: doctor.room, shift: doctor.shift });

  const { passwordHash, ...safeDoctor } = doctor;
  res.status(201).json({ token, doctor: safeDoctor });
});

app.post("/auth/login", (req, res) => {
  const email = String(req.body.email || "").trim();
  const password = String(req.body.password || "").trim();

  if (!email || !password) {
    return res.status(400).json({ error: "email and password are required" });
  }

  const doctor = findDoctorByEmail(email);
  if (!doctor || !doctor.passwordHash || doctor.passwordHash !== hashPassword(password)) {
    return res.status(401).json({ error: "Invalid email or password" });
  }

  const token = crypto.randomUUID();
  authTokens.set(token, { id: doctor.id, name: doctor.name, email: doctor.email, role: doctor.role, room: doctor.room, shift: doctor.shift });

  const { passwordHash, ...safeDoctor } = doctor;
  res.json({ token, doctor: safeDoctor });
});

app.get("/auth/me", (req, res) => {
  const user = getAuthenticatedDoctor(req);
  if (!user) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  res.json({ doctor: user });
});

app.get("/", (req, res) => {
  res.send(
    "Patient monitoring backend is running. Use the frontend at http://127.0.0.1:5173/ or call /health, /data, /patients."
  );
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    mqttUrl,
    mqttTopic,
    telegramEnabled: Boolean(telegramApi && telegramChatId),
  });
});

app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
  if (telegramApi && telegramChatId) {
    console.log("Telegram alert bot enabled");
    setInterval(pollTelegramUpdates, 4000);
  }
});
