const ADJECTIVES = [
  "静かな", "素早い", "明るい", "涼しい", "温かい", "小さな", "大きな", "遠い",
  "近い", "澄んだ", "柔らかい", "鋭い", "陽気な", "穏やかな", "軽やかな", "力強い"
];

const ANIMALS = [
  "キツネ", "タヌキ", "ウサギ", "フクロウ", "リス", "カワウソ", "ハリネズミ",
  "ペンギン", "イルカ", "コアラ", "パンダ", "ツバメ", "カモシカ", "ヤマネコ"
];

function randomDeviceName(seed) {
  const a = ADJECTIVES[seed % ADJECTIVES.length];
  const b = ANIMALS[Math.floor(seed / ADJECTIVES.length) % ANIMALS.length];
  const n = (seed % 90) + 10;
  return `${a}${b}${n}`;
}

const PING_INTERVAL_MS = 5000;
const SESSION_TIMEOUT_MS = 12000;

// 画像モデレーションに使うVisionモデル。カタログ変更に備えて一箇所にまとめておく。
const MODERATION_MODEL = "@cf/meta/llama-3.2-11b-vision-instruct";
const MAX_IMAGE_BYTES = 15 * 1024 * 1024; // 15MB程度で足切り（Workerのメモリ/実行時間対策）

const MODERATION_PROMPT =
  "あなたは画像モデレーターです。この画像に、次のいずれかに該当する内容が写っているか判定してください。\n" +
  "1) 性的な内容（ヌード、性行為、際どい露出など）\n" +
  "2) 暴力的・グロテスクな内容（流血、遺体、拷問、重度の怪我など）\n" +
  "該当する場合は unsafe、該当しない場合は safe とだけ、他の文字を一切含めずに答えてください。";

async function moderateImage(env, arrayBuffer, mime) {
  const base64 = arrayBufferToBase64(arrayBuffer);
  const dataUrl = `data:${mime || "image/jpeg"};base64,${base64}`;

  const result = await env.AI.run(MODERATION_MODEL, {
    messages: [
      {
        role: "user",
        content: [
          { type: "text", text: MODERATION_PROMPT },
          { type: "image_url", image_url: { url: dataUrl } },
        ],
      },
    ],
  });

  const text = (result && (result.response || result.result || "")).toString().toLowerCase();
  // モデルが多少余計な文字を返しても "unsafe" を含んでいればNG扱いにする（安全側に倒す）
  const isUnsafe = text.includes("unsafe");
  return !isUnsafe;
}

function arrayBufferToBase64(buffer) {
  let binary = "";
  const bytes = new Uint8Array(buffer);
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize));
  }
  return btoa(binary);
}

export class NetworkRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Map();
    this.sweepTimer = null;
  }

  startSweeper() {
    if (this.sweepTimer) return;
    this.sweepTimer = setInterval(() => this.sweep(), PING_INTERVAL_MS);
  }

  stopSweeperIfIdle() {
    if (this.sessions.size === 0 && this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
  }

  sweep() {
    const now = Date.now();
    for (const session of Array.from(this.sessions.values())) {
      if (now - session.lastSeen > SESSION_TIMEOUT_MS) {
        this.dropSession(session.id, 1001, "timeout");
        continue;
      }
      try {
        session.socket.send(JSON.stringify({ type: "ping", t: now }));
      } catch (err) {
        this.dropSession(session.id, 1011, "send-failed");
      }
    }
    this.stopSweeperIfIdle();
  }

  dropSession(id, code, reason) {
    const session = this.sessions.get(id);
    if (!session) return;
    this.sessions.delete(id);
    try {
      session.socket.close(code, reason);
    } catch (err) {}
    this.broadcast({ type: "peer-left", id }, id);
    this.stopSweeperIfIdle();
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname.endsWith("/upload-image") && request.method === "POST") {
      return this.handleImageUpload(request);
    }

    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
      return new Response("WebSocket連携が必要です", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const id = crypto.randomUUID();
    const seed = Array.from(id).reduce((acc, ch) => acc + ch.charCodeAt(0), 0);
    const name = randomDeviceName(seed);

    const session = { id, name, socket: server, lastSeen: Date.now() };
    this.sessions.set(id, session);
    this.startSweeper();

    server.addEventListener("message", (event) => {
      session.lastSeen = Date.now();
      this.handleMessage(session, event.data);
    });

    const closeOrError = () => {
      this.dropSession(id, 1000, "closed");
    };
    server.addEventListener("close", closeOrError);
    server.addEventListener("error", closeOrError);

    server.send(JSON.stringify({ type: "welcome", id, name }));

    const peers = Array.from(this.sessions.values())
      .filter((s) => s.id !== id)
      .map((s) => ({ id: s.id, name: s.name }));
    server.send(JSON.stringify({ type: "peer-list", peers }));

    this.broadcast({ type: "peer-joined", id, name }, id);

    return new Response(null, { status: 101, webSocket: client });
  }

  handleMessage(session, raw) {
    let data;
    try {
      data = JSON.parse(raw);
    } catch (err) {
      return;
    }

    if (data.type === "ping") {
      try {
        session.socket.send(JSON.stringify({ type: "pong" }));
      } catch (err) {
        this.dropSession(session.id, 1011, "send-failed");
      }
      return;
    }

    if (data.type === "pong") {
      return;
    }

    if (data.type === "rename" && typeof data.name === "string") {
      const name = data.name.trim().slice(0, 40);
      if (!name) return;
      session.name = name;
      this.broadcast({ type: "peer-renamed", id: session.id, name }, session.id);
      return;
    }

    if (data.type === "signal" && typeof data.to === "string") {
      const target = this.sessions.get(data.to);
      if (target) {
        try {
          target.socket.send(
            JSON.stringify({
              type: "signal",
              from: session.id,
              fromName: session.name,
              payload: data.payload,
            })
          );
        } catch (err) {
          this.dropSession(target.id, 1011, "send-failed");
        }
      }
    }
  }

  async handleImageUpload(request) {
    const toId = request.headers.get("X-Target-Id") || "";
    const fromId = request.headers.get("X-Sender-Id") || "";
    let name = request.headers.get("X-File-Name") || "image";
    try {
      name = decodeURIComponent(name);
    } catch (err) {}
    const mime = request.headers.get("X-File-Mime") || "application/octet-stream";

    const target = this.sessions.get(toId);
    const sender = this.sessions.get(fromId);

    if (!target || !sender) {
      return new Response(JSON.stringify({ ok: false, reason: "peer-not-found" }), {
        status: 404,
        headers: { "content-type": "application/json" },
      });
    }

    const arrayBuffer = await request.arrayBuffer();

    if (arrayBuffer.byteLength === 0 || arrayBuffer.byteLength > MAX_IMAGE_BYTES) {
      return new Response(JSON.stringify({ ok: false, reason: "size" }), {
        status: 413,
        headers: { "content-type": "application/json" },
      });
    }

    let safe;
    try {
      safe = await moderateImage(this.env, arrayBuffer, mime);
    } catch (err) {
      // 判定自体に失敗した場合は安全側に倒して送信をブロックする
      return new Response(JSON.stringify({ ok: false, reason: "moderation-error" }), {
        status: 502,
        headers: { "content-type": "application/json" },
      });
    }

    if (!safe) {
      return new Response(JSON.stringify({ ok: false, reason: "unsafe" }), {
        status: 422,
        headers: { "content-type": "application/json" },
      });
    }

    const base64 = arrayBufferToBase64(arrayBuffer);
    try {
      target.socket.send(
        JSON.stringify({
          type: "incoming-image",
          from: fromId,
          fromName: sender.name,
          name,
          mime,
          size: arrayBuffer.byteLength,
          data: base64,
        })
      );
    } catch (err) {
      this.dropSession(target.id, 1011, "send-failed");
      return new Response(JSON.stringify({ ok: false, reason: "delivery-failed" }), {
        status: 502,
        headers: { "content-type": "application/json" },
      });
    }

    return new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }

  broadcast(message, excludeId) {
    const text = JSON.stringify(message);
    for (const session of this.sessions.values()) {
      if (session.id === excludeId) continue;
      try {
        session.socket.send(text);
      } catch (err) {
        this.dropSession(session.id, 1011, "send-failed");
      }
    }
  }
}

function expandIpv6(ip) {
  let addr = ip.split("%")[0];
  const v4Match = addr.match(/(\d+\.\d+\.\d+\.\d+)$/);
  if (v4Match) {
    const octets = v4Match[1].split(".").map(Number);
    const hex1 = ((octets[0] << 8) | octets[1]).toString(16);
    const hex2 = ((octets[2] << 8) | octets[3]).toString(16);
    addr = addr.replace(v4Match[1], `${hex1}:${hex2}`);
  }

  const halves = addr.split("::");
  const head = halves[0] ? halves[0].split(":") : [];
  const tail = halves.length > 1 && halves[1] ? halves[1].split(":") : [];
  const missing = 8 - head.length - tail.length;
  const groups = halves.length > 1
    ? [...head, ...Array(Math.max(missing, 0)).fill("0"), ...tail]
    : head;

  return groups.map((g) => g.padStart(4, "0"));
}

function networkIdFromRequest(request) {
  const ip = (request.headers.get("CF-Connecting-IP") || "unknown").trim();

  if (ip.includes(":")) {
    const groups = expandIpv6(ip);
    if (groups.length === 8) {
      return "v6_" + groups.slice(0, 4).join("");
    }
  }

  return "v4_" + ip.replace(/[^a-zA-Z0-9.]/g, "_");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/ws" || url.pathname === "/upload-image") {
      const networkId = networkIdFromRequest(request);
      const objId = env.NETWORK_ROOM.idFromName(networkId);
      const stub = env.NETWORK_ROOM.get(objId);
      return stub.fetch(request);
    }

    return new Response("Not found", { status: 404 });
  },
};
